package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
)

// stubLauncher is a provider that completes immediately, so the envtest exercises
// the controller wiring (CRD -> resolve class -> dispatch -> status) deterministically,
// independent of any real substrate.
type stubLauncher struct{}

func (stubLauncher) Launch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	return aei.Handle{Provider: "envtest-stub", Ref: spec.ID}, nil
}

func init() {
	aei.Register("envtest-stub", func(aei.Config) (aei.Launcher, error) { return stubLauncher{}, nil })
}

var (
	testEnv   *envtest.Environment
	k8sClient client.Client
)

func TestMain(m *testing.M) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aeiv1.AddToScheme(scheme))

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		panic("start envtest: " + err.Error())
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic("build client: " + err.Error())
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		panic("build manager: " + err.Error())
	}

	reconciler := &AgentRunReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Plane:  control.New(control.Config{Secret: []byte("envtest-secret")}),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		panic("setup reconciler: " + err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = mgr.Start(ctx) }()

	code := m.Run()

	cancel()
	_ = testEnv.Stop()
	os.Exit(code)
}

// TestAgentRunReconcilesToSucceeded drives the full CRD path: an AgentRun bound to
// a class whose provider is the stub reaches Succeeded, with the provider recorded
// on its status handle.
func TestAgentRunReconcilesToSucceeded(t *testing.T) {
	ctx := context.Background()

	class := &aeiv1.AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "stub-class"},
		Spec:       aeiv1.AgentProviderClassSpec{Provider: "envtest-stub"},
	}
	if err := k8sClient.Create(ctx, class); err != nil {
		t.Fatalf("create class: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, class) })

	run := &aeiv1.AgentRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-succeed", Namespace: "default"},
		Spec: aeiv1.AgentRunSpec{
			ProviderClassName: "stub-class",
			TaskRef:           "worklist://item/x",
			TimeoutSeconds:    30,
		},
	}
	if err := k8sClient.Create(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	got := waitForPhase(t, "run-succeed", 20*time.Second)
	if got.Status.Phase != aeiv1.PhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded (status=%+v)", got.Status.Phase, got.Status)
	}
	if got.Status.Handle.Provider != "envtest-stub" {
		t.Errorf("handle.provider = %q, want envtest-stub", got.Status.Handle.Provider)
	}
}

// TestAgentRunFailsOnUnknownClass exercises the failure path: an AgentRun pointing
// at a missing class is marked Failed rather than hanging.
func TestAgentRunFailsOnUnknownClass(t *testing.T) {
	ctx := context.Background()
	run := &aeiv1.AgentRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-noclass", Namespace: "default"},
		Spec:       aeiv1.AgentRunSpec{ProviderClassName: "does-not-exist", TimeoutSeconds: 30},
	}
	if err := k8sClient.Create(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	got := waitForPhase(t, "run-noclass", 20*time.Second)
	if got.Status.Phase != aeiv1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
}

func waitForPhase(t *testing.T, name string, within time.Duration) aeiv1.AgentRun {
	t.Helper()
	var got aeiv1.AgentRun
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, &got); err == nil {
			if got.Status.Phase == aeiv1.PhaseSucceeded || got.Status.Phase == aeiv1.PhaseFailed {
				return got
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run %q did not reach a terminal phase in %s (phase=%q)", name, within, got.Status.Phase)
	return got
}
