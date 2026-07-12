package sanity

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"

	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	"github.com/jackfrancis/agent-execution-interface/providers/agentsandbox"
	"github.com/jackfrancis/agent-execution-interface/providers/k8sjob"
)

var sandboxGVR = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}

// TestCrossProviderCallbackCompletion drives k8s-job and agent-sandbox through the
// same control plane and asserts the same terminal outcome, arrived at the same
// way — the runtime's /complete callback. It also proves the push-token injection
// round-trip: the token the plane minted is read back out of the workload the
// provider created, then used to authorize the callback.
func TestCrossProviderCallbackCompletion(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		make func(controlURL string) (aei.Launcher, func() string) // launcher + a reader for the injected token
	}{
		{"k8s-job", func(controlURL string) (aei.Launcher, func() string) {
			client := fake.NewSimpleClientset()
			l := k8sjob.New(client, k8sjob.Options{Namespace: "aei", Image: "img:1", ControlURL: controlURL})
			return l, func() string {
				jobs, err := client.BatchV1().Jobs("aei").List(ctx, metav1.ListOptions{})
				if err != nil || len(jobs.Items) == 0 {
					return ""
				}
				return envValue(jobs.Items[0].Spec.Template.Spec.Containers[0].Env, aei.EnvCredential)
			}
		}},
		{"agent-sandbox", func(controlURL string) (aei.Launcher, func() string) {
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
				map[schema.GroupVersionResource]string{sandboxGVR: "SandboxList"})
			l := agentsandbox.New(client, agentsandbox.Options{Namespace: "aei", Image: "img:1", ControlURL: controlURL})
			return l, func() string {
				list, err := client.Resource(sandboxGVR).Namespace("aei").List(ctx, metav1.ListOptions{})
				if err != nil || len(list.Items) == 0 {
					return ""
				}
				return sandboxEnv(&list.Items[0], aei.EnvCredential)
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The server delegates to the plane, which is assigned just below —
			// breaking the provider→URL→server→plane→provider construction cycle.
			var plane *control.Plane
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				plane.Handler().ServeHTTP(w, r)
			}))
			defer srv.Close()

			launcher, injectedToken := tc.make(srv.URL + "/aei/v1alpha1")
			plane = control.New(control.Config{Secret: []byte("sanity-secret")})

			spec := aei.RunSpec{
				ID:         "run-" + tc.name,
				TaskRef:    "worklist://item/x",
				Parameters: map[string]string{"kind": "smoke"},
				Deadline:   3 * time.Second,
			}
			resCh := make(chan aei.Result, 1)
			go func() {
				r, _ := plane.Run(ctx, launcher, spec)
				resCh <- r
			}()

			// Wait until the provider has created the workload, then read the token
			// the plane injected into it (the injection round-trip).
			token := waitFor(t, injectedToken)

			// Act as the runtime: report success through the callback.
			body, _ := json.Marshal(map[string]any{"run_id": spec.ID, "result_ref": "res://ok"})
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/aei/v1alpha1/complete", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("callback: %v", err)
			}
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("callback status = %d, want 202", resp.StatusCode)
			}

			select {
			case r := <-resCh:
				if r.Err != nil {
					t.Errorf("want success, got %v", r.Err)
				}
				if r.ResultRef != "res://ok" {
					t.Errorf("resultRef = %q, want res://ok", r.ResultRef)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Run did not finalize after the callback")
			}
		})
	}
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func sandboxEnv(u *unstructured.Unstructured, name string) string {
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "podTemplate", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		return ""
	}
	c, ok := containers[0].(map[string]any)
	if !ok {
		return ""
	}
	envs, _ := c["env"].([]any)
	for _, e := range envs {
		m, ok := e.(map[string]any)
		if !ok {
			continue
		}
		if m["name"] == name {
			if v, ok := m["value"].(string); ok {
				return v
			}
		}
	}
	return ""
}

func waitFor(t *testing.T, get func() string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v := get(); v != "" {
			return v
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the provider to create the workload")
	return ""
}
