package k8sjob

import (
	"context"
	"strings"
	"testing"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// TestDispatchSetsRuntimeClassFromIsolation is the regression check for the silent
// downgrade: a run that names an isolation class must reach the kubelet as that
// RuntimeClass, not as the default runtime.
func TestDispatchSetsRuntimeClassFromIsolation(t *testing.T) {
	l := testLauncher()
	if _, err := l.client.NodeV1().RuntimeClasses().Create(context.Background(),
		&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}, Handler: "runsc"},
		metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed runtimeclass: %v", err)
	}

	spec := aei.RunSpec{ID: "run-1", TaskRef: "task", Isolation: "gvisor"}
	if _, err := l.Dispatch(context.Background(), spec, aei.Credential{Value: "tok"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	job, err := l.client.BatchV1().Jobs("aei").Get(context.Background(), "aei-run-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	rc := job.Spec.Template.Spec.RuntimeClassName
	if rc == nil || *rc != "gvisor" {
		t.Fatalf("runtimeClassName = %v, want gvisor", rc)
	}
}

// TestDispatchWithoutIsolationLeavesDefaultRuntime pins the other half: asking for
// no isolation must not invent a RuntimeClass.
func TestDispatchWithoutIsolationLeavesDefaultRuntime(t *testing.T) {
	l := testLauncher()
	spec := aei.RunSpec{ID: "run-1", TaskRef: "task"}
	if _, err := l.Dispatch(context.Background(), spec, aei.Credential{Value: "tok"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	job, err := l.client.BatchV1().Jobs("aei").Get(context.Background(), "aei-run-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rc := job.Spec.Template.Spec.RuntimeClassName; rc != nil {
		t.Fatalf("runtimeClassName = %q, want unset", *rc)
	}
}

// TestDispatchRejectsUnsatisfiableIsolation is the security-relevant case: the
// cluster has no such RuntimeClass, so the run must fail at dispatch and leave no
// Job behind — never run unisolated while claiming isolation.
func TestDispatchRejectsUnsatisfiableIsolation(t *testing.T) {
	l := testLauncher()
	spec := aei.RunSpec{ID: "run-1", TaskRef: "task", Isolation: "gvisor"}

	_, err := l.Dispatch(context.Background(), spec, aei.Credential{Value: "tok"})
	if err == nil {
		t.Fatal("dispatch succeeded, want isolation error")
	}
	if !strings.Contains(err.Error(), "gvisor") {
		t.Fatalf("error %q does not name the isolation class", err)
	}
	jobs, lerr := l.client.BatchV1().Jobs("aei").List(context.Background(), metav1.ListOptions{})
	if lerr != nil {
		t.Fatalf("list jobs: %v", lerr)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("created %d job(s) despite unsatisfiable isolation", len(jobs.Items))
	}
}
