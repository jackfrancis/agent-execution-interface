package agentsandbox

import (
	"context"
	"strings"
	"testing"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// TestDispatchSetsRuntimeClassFromIsolation pins that a Sandbox resolves isolation
// the same way a Job does. A Sandbox has no isolation field of its own — it carries
// a PodSpec — so the isolation class must land on that pod's runtimeClassName.
func TestDispatchSetsRuntimeClassFromIsolation(t *testing.T) {
	l := testLauncher()
	l.kube = kubefake.NewSimpleClientset(&nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}, Handler: "runsc",
	})

	spec := aei.RunSpec{ID: "run-1", TaskRef: "task", Isolation: "gvisor"}
	if _, err := l.Dispatch(context.Background(), spec, aei.Credential{Value: "tok"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	got, err := l.client.Resource(sandboxGVR).Namespace("aei").
		Get(context.Background(), "aei-run-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get sandbox: %v", err)
	}
	rc, found, err := unstructured.NestedString(got.Object, "spec", "podTemplate", "spec", "runtimeClassName")
	if err != nil || !found {
		t.Fatalf("runtimeClassName not set on the sandbox pod template (found=%v err=%v)", found, err)
	}
	if rc != "gvisor" {
		t.Fatalf("runtimeClassName = %q, want gvisor", rc)
	}
}

// TestDispatchRejectsUnsatisfiableIsolation: no gvisor RuntimeClass in the cluster,
// so the run fails at dispatch and no Sandbox is created.
func TestDispatchRejectsUnsatisfiableIsolation(t *testing.T) {
	l := testLauncher()
	l.kube = kubefake.NewSimpleClientset()

	_, err := l.Dispatch(context.Background(),
		aei.RunSpec{ID: "run-1", TaskRef: "task", Isolation: "gvisor"},
		aei.Credential{Value: "tok"})
	if err == nil {
		t.Fatal("dispatch succeeded, want isolation error")
	}
	if !strings.Contains(err.Error(), "gvisor") {
		t.Fatalf("error %q does not name the isolation class", err)
	}
	list, lerr := l.client.Resource(sandboxGVR).Namespace("aei").List(context.Background(), metav1.ListOptions{})
	if lerr != nil {
		t.Fatalf("list sandboxes: %v", lerr)
	}
	if len(list.Items) != 0 {
		t.Fatalf("created %d sandbox(es) despite unsatisfiable isolation", len(list.Items))
	}
}
