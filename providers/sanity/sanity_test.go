package sanity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"

	dynamicfake "k8s.io/client-go/dynamic/fake"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	"github.com/jackfrancis/agent-execution-interface/providers/agentsandbox"
	"github.com/jackfrancis/agent-execution-interface/providers/k8sjob"
	"github.com/jackfrancis/agent-execution-interface/providers/substrate"
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

// TestAgentSubstratePullCredentialCompletion extends the cross-provider conformance to
// the durable agent-substrate shape, which differs from the push/workload providers on
// BOTH axes yet must reach the SAME terminal outcome: the credential is delivered over
// A2A message metadata (not workload env), and it is a PULL credential — a single-use
// ticket the runtime redeems for a token before completing. It asserts the pull
// round-trip (the plane mints a ticket → it rides the dispatch metadata → /redeem → the
// token authorizes /complete) and that the run finalizes to Succeeded through the same
// plane callback as every other provider.
func TestAgentSubstratePullCredentialCompletion(t *testing.T) {
	ctx := context.Background()

	var plane *control.Plane
	planeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plane.Handler().ServeHTTP(w, r)
	}))
	defer planeSrv.Close()

	// The router stands in for atenet-router + the durable actor's serve loop: it accepts
	// the A2A message and, detached (as the real actor does), plays the pull-credential
	// runtime — redeem the ticket carried in the metadata, then report completion.
	completed := make(chan error, 1)
	routerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Params struct {
				Message struct {
					Metadata map[string]string `json:"metadata"`
				} `json:"message"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "bad a2a message", http.StatusBadRequest)
			return
		}
		md := msg.Params.Message.Metadata
		// Ack immediately (any state but "failed" = accepted), then run detached.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": msg.ID,
			"result": map[string]any{"id": "task-" + md[aei.EnvRunID], "status": map[string]any{"state": "submitted"}},
		})
		go func() { completed <- redeemAndComplete(md) }()
	}))
	defer routerSrv.Close()

	launcher := substrate.New(&http.Client{}, substrate.Options{
		RouterURL:  routerSrv.URL,
		ActorHost:  "aei-runtime.aei.test", // a literal host ⇒ ensure() is a no-op (no cluster here)
		ControlURL: planeSrv.URL + "/aei/v1alpha1",
	})
	plane = control.New(control.Config{Secret: []byte("sanity-secret")})

	resCh := make(chan aei.Result, 1)
	go func() {
		r, _ := plane.Run(ctx, launcher, aei.RunSpec{
			ID:       "run-agent-substrate",
			TaskRef:  "worklist://item/x",
			Deadline: 3 * time.Second,
		})
		resCh <- r
	}()

	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("pull-credential runtime failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never redeemed + completed")
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
}

// redeemAndComplete plays the durable, pull-credential runtime: redeem the ticket from
// the dispatch metadata for a run token, then report completion with it — the exact
// /redeem → /complete bracket the runtime SDK performs (docs/runtime-abi.md §2).
func redeemAndComplete(md map[string]string) error {
	ctrlURL := md[aei.EnvControlURL]
	broker := md[aei.EnvBrokerURL]
	if broker == "" {
		broker = ctrlURL // AEI_BROKER_URL is omitted when it equals the control URL
	}
	ticket := md[aei.EnvTicket]
	if ticket == "" {
		return fmt.Errorf("dispatch metadata carried no %s (expected a pull ticket)", aei.EnvTicket)
	}

	rb, _ := json.Marshal(map[string]string{"ticket": ticket})
	resp, err := http.Post(broker+"/redeem", "application/json", bytes.NewReader(rb))
	if err != nil {
		return fmt.Errorf("redeem: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("redeem: status %d", resp.StatusCode)
	}
	var red struct {
		Credential string `json:"credential"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&red); err != nil {
		return fmt.Errorf("redeem decode: %w", err)
	}
	if red.Credential == "" {
		return fmt.Errorf("redeem returned an empty credential")
	}

	cb, _ := json.Marshal(map[string]string{"run_id": md[aei.EnvRunID], "result_ref": "res://ok"})
	req, _ := http.NewRequest(http.MethodPost, ctrlURL+"/complete", bytes.NewReader(cb))
	req.Header.Set("Authorization", "Bearer "+red.Credential)
	cresp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	defer cresp.Body.Close()
	if cresp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("complete: status %d", cresp.StatusCode)
	}
	return nil
}

// TestWatchBackstopCompletion drives runs to completion by the PROVIDER's watch, with
// NO runtime callback — the second of the three completion signals the control plane
// races (callback · watch · deadline). It runs EVERY pod-shaped provider through the
// same claim, because the dimension is only as good as its least-covered provider:
// agent-sandbox went a long time with a detached Await, so a sandbox that failed
// before its runtime could start was reported as a bare deadline timeout with no
// cause. A provider must therefore finalize such a run AND carry the substrate's
// reason out with it.
func TestWatchBackstopCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		build   func() (aei.Launcher, func(t *testing.T, ctx context.Context))
		wantErr string // substring of the expected failure; empty means expect success
	}{
		{
			name: "k8s-job/completed",
			build: func() (aei.Launcher, func(*testing.T, context.Context)) {
				c := fake.NewSimpleClientset()
				return k8sjob.New(c, k8sjob.Options{Namespace: "aei", Image: "img:1"}),
					func(t *testing.T, ctx context.Context) { seedJob(t, ctx, c, batchv1.JobComplete) }
			},
		},
		{
			name: "k8s-job/failed",
			build: func() (aei.Launcher, func(*testing.T, context.Context)) {
				c := fake.NewSimpleClientset()
				return k8sjob.New(c, k8sjob.Options{Namespace: "aei", Image: "img:1"}),
					func(t *testing.T, ctx context.Context) { seedJob(t, ctx, c, batchv1.JobFailed) }
			},
			wantErr: "run failed",
		},
		{
			name: "agent-sandbox/pod-succeeded",
			build: func() (aei.Launcher, func(*testing.T, context.Context)) {
				c := newSandboxClient()
				return agentsandbox.New(c, agentsandbox.Options{Namespace: "aei", Image: "img:1"}),
					func(t *testing.T, ctx context.Context) { seedSandbox(t, ctx, c, "PodSucceeded", "Pod succeeded") }
			},
		},
		{
			// The case observed in a real cluster: the node ran out of task budget,
			// the sandbox never started, so no runtime existed to call back. Only the
			// Sandbox's condition carries the cause.
			name: "agent-sandbox/pod-failed",
			build: func() (aei.Launcher, func(*testing.T, context.Context)) {
				c := newSandboxClient()
				return agentsandbox.New(c, agentsandbox.Options{Namespace: "aei", Image: "img:1"}),
					func(t *testing.T, ctx context.Context) { seedSandbox(t, ctx, c, "PodFailed", "Pod failed") }
			},
			wantErr: "Pod failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			l, seed := tc.build()
			plane := control.New(control.Config{Secret: []byte("sanity-secret")})

			spec := aei.RunSpec{ID: "run-watch", TaskRef: "worklist://item/x", Deadline: 15 * time.Second}
			resCh := make(chan aei.Result, 1)
			go func() {
				r, _ := plane.Run(ctx, l, spec)
				resCh <- r
			}()

			// Seed the terminal state the substrate would set. No callback is ever
			// sent, so only the provider's watch can finalize this run.
			seed(t, ctx)

			select {
			case r := <-resCh:
				switch {
				case tc.wantErr == "" && r.Err != nil:
					t.Errorf("want success via the watch backstop, got %v", r.Err)
				case tc.wantErr != "" && r.Err == nil:
					t.Errorf("want a failure containing %q via the watch backstop, got success", tc.wantErr)
				case tc.wantErr != "" && !strings.Contains(r.Err.Error(), tc.wantErr):
					t.Errorf("error %q does not carry the substrate's cause (%q)", r.Err, tc.wantErr)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("run did not finalize via the watch backstop (it would have waited out its deadline)")
			}
		})
	}
}

func newSandboxClient() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{sandboxGVR: "SandboxList"})
}

// seedJob waits for the dispatched Job, then sets the terminal condition the
// Kubernetes Job controller would set.
func seedJob(t *testing.T, ctx context.Context, client *fake.Clientset, cond batchv1.JobConditionType) {
	t.Helper()
	job := waitForJob(t, client, "aei")
	job.Status.Conditions = []batchv1.JobCondition{{Type: cond, Status: corev1.ConditionTrue, Reason: "test"}}
	if _, err := client.BatchV1().Jobs("aei").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("seed status: %v", err)
	}
}

// seedSandbox waits for the dispatched Sandbox, then sets the Finished condition
// the agent-sandbox controller would set when the backing pod reaches a terminal
// phase.
func seedSandbox(t *testing.T, ctx context.Context, client *dynamicfake.FakeDynamicClient, reason, message string) {
	t.Helper()
	sb := waitForSandbox(t, client, "aei")
	conds := []any{map[string]any{
		"type": "Finished", "status": "True", "reason": reason, "message": message,
	}}
	if err := unstructured.SetNestedSlice(sb.Object, conds, "status", "conditions"); err != nil {
		t.Fatalf("set conditions: %v", err)
	}
	if _, err := client.Resource(sandboxGVR).Namespace("aei").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("seed status: %v", err)
	}
}

func waitForSandbox(t *testing.T, client *dynamicfake.FakeDynamicClient, ns string) *unstructured.Unstructured {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		list, err := client.Resource(sandboxGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
		if err == nil && len(list.Items) > 0 {
			return &list.Items[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no Sandbox was dispatched")
	return nil
}

// TestDeadlineEnforcement pins the universal backstop: a run that neither calls back
// nor completes via watch finalizes at its deadline — no run outlives its budget. The
// Sandbox here is dispatched but never reaches a terminal condition, so neither of the
// first two signals can fire and only the deadline remains.
func TestDeadlineEnforcement(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{sandboxGVR: "SandboxList"})
	l := agentsandbox.New(client, agentsandbox.Options{Namespace: "aei", Image: "img:1"})
	plane := control.New(control.Config{Secret: []byte("sanity-secret")})

	start := time.Now()
	r, _ := plane.Run(ctx, l, aei.RunSpec{ID: "run-deadline", TaskRef: "worklist://item/x", Deadline: 300 * time.Millisecond})
	if r.Err == nil {
		t.Fatal("want a deadline error for a run that never completes, got success")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("deadline enforcement took %v, want it to fire near the 300ms budget", elapsed)
	}
}

func waitForJob(t *testing.T, client *fake.Clientset, ns string) *batchv1.Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := client.BatchV1().Jobs(ns).List(context.Background(), metav1.ListOptions{})
		if err == nil && len(jobs.Items) > 0 {
			return &jobs.Items[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the provider to create the Job")
	return nil
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
