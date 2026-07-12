package agentsandbox

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

func testLauncher() *Launcher {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{sandboxGVR: "SandboxList"})
	l := New(client, Options{
		Namespace:      "aei",
		Image:          "img:1",
		ServiceAccount: "aei-runtime",
		ControlURL:     "http://cp/aei/v1alpha1",
	})
	l.now = func() time.Time { return time.UnixMilli(1789000000000) }
	return l
}

// podEnv digs the container env out of the unstructured spec.podTemplate.spec.
func podEnv(t *testing.T, u *unstructured.Unstructured) map[string]string {
	t.Helper()
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "podTemplate", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		t.Fatalf("no containers in podTemplate: found=%v err=%v", found, err)
	}
	c := containers[0].(map[string]any)
	env := map[string]string{}
	for _, e := range c["env"].([]any) {
		m := e.(map[string]any)
		if v, ok := m["value"].(string); ok {
			env[m["name"].(string)] = v
		}
	}
	return env
}

// TestSandboxEmbedsInjectionContract is the cross-provider regression check: the
// Sandbox carries the identical runtime container + AEI_* injection contract as
// k8s-job, inside spec.podTemplate.spec.
func TestSandboxEmbedsInjectionContract(t *testing.T) {
	l := testLauncher()
	spec := aei.RunSpec{
		ID:         "run-abc",
		TaskRef:    "worklist://item/pr-4127",
		Parameters: map[string]string{"kind": "converse"},
		Deadline:   5 * time.Minute,
		Identity:   aei.IdentityRequest{Audience: "team-a-agent"},
	}
	u := l.sandbox(spec, aei.Credential{Value: "tok-123", Kind: aei.Token}, l.now())

	if u.GetAPIVersion() != sandboxAPIVersion || u.GetKind() != sandboxKind {
		t.Fatalf("gvk = %s/%s, want %s/%s", u.GetAPIVersion(), u.GetKind(), sandboxAPIVersion, sandboxKind)
	}
	if u.GetName() != "aei-run-abc" {
		t.Errorf("name = %q, want aei-run-abc", u.GetName())
	}

	env := podEnv(t, u)
	want := map[string]string{
		aei.EnvRunID:        "run-abc",
		aei.EnvTaskRef:      "worklist://item/pr-4127",
		aei.EnvParams:       `{"kind":"converse"}`,
		aei.EnvControlURL:   "http://cp/aei/v1alpha1",
		aei.EnvCredential:   "tok-123",
		aei.EnvDispatchedAt: "1789000000000",
		aei.EnvDeadline:     "1789000300000",
		aei.EnvAudience:     "team-a-agent",
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("env[%s] = %q, want %q", k, env[k], v)
		}
	}
	if _, ok := env[aei.EnvTicket]; ok {
		t.Error("push credential must not set AEI_RUN_TICKET")
	}
}

// TestSelfReapWhenDeadlineSet verifies the native scheduled deletion is stamped
// from the run deadline so a finished Sandbox cleans itself up.
func TestSelfReapWhenDeadlineSet(t *testing.T) {
	l := testLauncher()
	u := l.sandbox(aei.RunSpec{ID: "r", Deadline: time.Minute}, aei.Credential{Value: "tok"}, l.now())

	shutdown, found, err := unstructured.NestedString(u.Object, "spec", "lifecycle", "shutdownTime")
	if err != nil || !found || shutdown == "" {
		t.Fatalf("shutdownTime not set: found=%v err=%v", found, err)
	}
	policy, _, _ := unstructured.NestedString(u.Object, "spec", "lifecycle", "shutdownPolicy")
	if policy != "Delete" {
		t.Errorf("shutdownPolicy = %q, want Delete", policy)
	}
}

// TestDispatchCreatesSandbox proves Dispatch submits a Sandbox via the dynamic
// client and returns an agent-sandbox handle naming it.
func TestDispatchCreatesSandbox(t *testing.T) {
	l := testLauncher()
	h, err := l.Dispatch(context.Background(),
		aei.RunSpec{ID: "run-1"}, aei.Credential{Value: "tok"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if h.Provider != "agent-sandbox" || h.Ref != "aei-run-1" {
		t.Fatalf("handle = %+v, want {agent-sandbox aei-run-1}", h)
	}
	got, err := l.client.Resource(sandboxGVR).Namespace("aei").Get(context.Background(), "aei-run-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get sandbox: %v", err)
	}
	if got.GetKind() != sandboxKind {
		t.Errorf("created kind = %q, want Sandbox", got.GetKind())
	}
}

// TestAwaitDetachedReturnsOnDeadline verifies the detached Await blocks until the
// context deadline (the backstop) and reports a timeout as Result.Err.
func TestAwaitDetachedReturnsOnDeadline(t *testing.T) {
	l := testLauncher()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	res, err := l.Await(ctx, aei.Handle{Provider: "agent-sandbox", Ref: "aei-run-1"})
	if err != nil {
		t.Fatalf("Await operational error: %v", err)
	}
	if res.Err == nil {
		t.Error("want Result.Err (timeout) from the detached Await")
	}
}

func TestRegisteredUnderName(t *testing.T) {
	found := false
	for _, n := range aei.Names() {
		if n == "agent-sandbox" {
			found = true
		}
	}
	if !found {
		t.Fatalf("agent-sandbox not registered; got %v", aei.Names())
	}
}
