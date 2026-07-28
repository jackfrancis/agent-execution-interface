package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
)

// fakeWarmer is a Launcher that records Warm calls (satisfies aei.Warmer).
type fakeWarmer struct {
	warmed bool
	spec   aei.RunSpec
}

func (f *fakeWarmer) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (f *fakeWarmer) Warm(_ context.Context, spec aei.RunSpec) error {
	f.warmed, f.spec = true, spec
	return nil
}

// plainLauncher is a Launcher that is NOT a Warmer (a per-run provider).
type plainLauncher struct{}

func (plainLauncher) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}

func warmScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := aeiv1.AddToScheme(s); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}
	return s
}

// TestAgentAppWarmsDurable: an app bound to a class whose provider is a Warmer gets
// warmed with its app name, namespace, and image.
func TestAgentAppWarmsDurable(t *testing.T) {
	scheme := warmScheme(t)
	app := &aeiv1.AgentApp{
		ObjectMeta: metav1.ObjectMeta{Name: "gofer", Namespace: "default"},
		Spec:       aeiv1.AgentAppSpec{ProviderClassName: "durable", Image: "reg/gofer@sha256:abc"},
	}
	class := &aeiv1.AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "durable"},
		Spec:       aeiv1.AgentProviderClassSpec{Provider: "agent-substrate"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(app, class).Build()

	fw := &fakeWarmer{}
	r := &AgentAppReconciler{Client: c, Scheme: scheme,
		build: func(*aeiv1.AgentProviderClass) (aei.Launcher, error) { return fw, nil }}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gofer", Namespace: "default"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !fw.warmed {
		t.Fatal("Warm was not called for a durable app")
	}
	if fw.spec.App != "gofer" || fw.spec.AppNamespace != "default" || fw.spec.Image != "reg/gofer@sha256:abc" {
		t.Errorf("Warm spec = %+v, want app=gofer ns=default image=reg/gofer@sha256:abc", fw.spec)
	}
}

// TestAgentAppSkipsNonWarmer: an app bound to a per-run provider (not a Warmer) is a
// clean no-op — no error, nothing provisioned ahead of time.
func TestAgentAppSkipsNonWarmer(t *testing.T) {
	scheme := warmScheme(t)
	app := &aeiv1.AgentApp{
		ObjectMeta: metav1.ObjectMeta{Name: "pyfer", Namespace: "default"},
		Spec:       aeiv1.AgentAppSpec{ProviderClassName: "isolated", Image: "reg/pyfer@sha256:abc"},
	}
	class := &aeiv1.AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "isolated"},
		Spec:       aeiv1.AgentProviderClassSpec{Provider: "k8s-job"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(app, class).Build()

	r := &AgentAppReconciler{Client: c, Scheme: scheme,
		build: func(*aeiv1.AgentProviderClass) (aei.Launcher, error) { return plainLauncher{}, nil }}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pyfer", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (a non-warmer provider is a clean no-op)", res.RequeueAfter)
	}
}

// TestAgentAppNoImageSkips: an app without an image has nothing to warm.
func TestAgentAppNoImageSkips(t *testing.T) {
	scheme := warmScheme(t)
	app := &aeiv1.AgentApp{
		ObjectMeta: metav1.ObjectMeta{Name: "noimg", Namespace: "default"},
		Spec:       aeiv1.AgentAppSpec{ProviderClassName: "durable"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(app).Build()

	fw := &fakeWarmer{}
	r := &AgentAppReconciler{Client: c, Scheme: scheme,
		build: func(*aeiv1.AgentProviderClass) (aei.Launcher, error) { return fw, nil }}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "noimg", Namespace: "default"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fw.warmed {
		t.Error("must not warm an app with no image")
	}
}
