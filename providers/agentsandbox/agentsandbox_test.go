package agentsandbox

import (
	"context"
	"errors"
	"strings"
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
	l.pollInterval = time.Millisecond
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

	// The Sandbox API has these directly under spec. Anywhere else the API server
	// prunes them and the Sandbox is never reaped.
	shutdown, found, err := unstructured.NestedString(u.Object, "spec", "shutdownTime")
	if err != nil || !found || shutdown == "" {
		t.Fatalf("spec.shutdownTime not set: found=%v err=%v", found, err)
	}
	if want := l.now().Add(time.Minute + shutdownGrace).UTC().Format(time.RFC3339); shutdown != want {
		t.Errorf("spec.shutdownTime = %q, want %q (deadline + grace)", shutdown, want)
	}
	policy, _, _ := unstructured.NestedString(u.Object, "spec", "shutdownPolicy")
	if policy != "Delete" {
		t.Errorf("spec.shutdownPolicy = %q, want Delete", policy)
	}
	if _, found, _ := unstructured.NestedMap(u.Object, "spec", "lifecycle"); found {
		t.Error("spec.lifecycle is not part of the Sandbox API and must not be set")
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

// seedCondition puts a status condition on an existing Sandbox, standing in for
// the agent-sandbox controller observing the backing pod.
func seedCondition(t *testing.T, l *Launcher, ns, name, condType, status, reason, msg string) {
	t.Helper()
	got, err := l.client.Resource(sandboxGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get sandbox: %v", err)
	}
	conds := []any{map[string]any{
		"type": condType, "status": status, "reason": reason, "message": msg,
	}}
	if err := unstructured.SetNestedSlice(got.Object, conds, "status", "conditions"); err != nil {
		t.Fatalf("set conditions: %v", err)
	}
	if _, err := l.client.Resource(sandboxGVR).Namespace(ns).Update(context.Background(), got, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update sandbox: %v", err)
	}
}

// dispatchOne creates a Sandbox for a run and returns its Handle.
func dispatchOne(t *testing.T, l *Launcher) aei.Handle {
	t.Helper()
	h, err := l.Dispatch(context.Background(), aei.RunSpec{ID: "run-1", TaskRef: "task"}, aei.Credential{Value: "tok"})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	return h
}

// TestAwaitWatchReportsPodFailure is the reason Await watches at all: a run whose
// pod never starts can never send a /complete callback, so without the watch it
// would sit until its deadline and be reported as a timeout with no cause. The
// Sandbox's Finished/PodFailed condition is the only place that failure appears.
func TestAwaitWatchReportsPodFailure(t *testing.T) {
	l := testLauncher()
	h := dispatchOne(t, l)
	seedCondition(t, l, h.Namespace, h.Ref, "Finished", "True", "PodFailed", "Pod failed")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := l.Await(ctx, h)
	if err != nil {
		t.Fatalf("Await operational error: %v", err)
	}
	if res.Err == nil {
		t.Fatal("want Result.Err from a failed Sandbox")
	}
	if !strings.Contains(res.Err.Error(), "Pod failed") {
		t.Errorf("error %q does not carry the Sandbox's reason", res.Err)
	}
	if errors.Is(res.Err, context.DeadlineExceeded) {
		t.Error("failure was reported as a timeout, not as the pod failure")
	}
}

// TestAwaitWatchReportsPodSuccess: a Sandbox whose pod completed is a successful
// terminal outcome, matching k8s-job's JobComplete.
func TestAwaitWatchReportsPodSuccess(t *testing.T) {
	l := testLauncher()
	h := dispatchOne(t, l)
	seedCondition(t, l, h.Namespace, h.Ref, "Finished", "True", "PodSucceeded", "Pod succeeded")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := l.Await(ctx, h)
	if err != nil {
		t.Fatalf("Await operational error: %v", err)
	}
	if res.Err != nil {
		t.Errorf("Result.Err = %v, want nil for a completed Sandbox", res.Err)
	}
}

// TestAwaitDeadlineBackstop: a live Sandbox with no terminal condition keeps the
// deadline as the universal backstop behind the watch.
func TestAwaitDeadlineBackstop(t *testing.T) {
	l := testLauncher()
	h := dispatchOne(t, l)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	res, err := l.Await(ctx, h)
	if err != nil {
		t.Fatalf("Await operational error: %v", err)
	}
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Errorf("Result.Err = %v, want the deadline backstop", res.Err)
	}
}

// TestAwaitSandboxGone: nothing can report the outcome of a run whose Sandbox no
// longer exists, so fail immediately instead of waiting out the deadline.
func TestAwaitSandboxGone(t *testing.T) {
	l := testLauncher()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := l.Await(ctx, aei.Handle{Provider: providerName, Ref: "aei-run-1", Namespace: "aei"})
	if err != nil {
		t.Fatalf("Await operational error: %v", err)
	}
	if res.Err == nil {
		t.Fatal("want Result.Err for a missing Sandbox")
	}
	if errors.Is(res.Err, context.DeadlineExceeded) {
		t.Error("a missing Sandbox should fail at once, not at the deadline")
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
