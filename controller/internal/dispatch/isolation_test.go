package dispatch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
)

// specCapture records the RunSpec a dispatch produced, so a test can assert what
// the provider was actually handed — the gap this covers is a field resolved by
// the control plane but never put on the wire.
type specCapture struct{ specs chan aei.RunSpec }

func (s *specCapture) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, _ := s.Dispatch(ctx, spec, cred)
	r, _ := s.Await(ctx, h)
	return h, r.Err
}
func (s *specCapture) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	s.specs <- spec
	return aei.Handle{Provider: "test-isolation", Ref: spec.ID}, nil
}
func (s *specCapture) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
}

var isolationLauncher = &specCapture{specs: make(chan aei.RunSpec, 8)}

func init() {
	aei.Register("test-isolation", func(aei.Config) (aei.Launcher, error) { return isolationLauncher, nil })
}

// newIsolationServer builds a data-plane server whose class carries the given
// isolation policy. Each test gets its own class name so the handler's per-class
// launcher cache never leaks between them.
func newIsolationServer(t *testing.T, className, defaultIso string, caps ...string) *httptest.Server {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := aeiv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	app := &aeiv1.AgentApp{
		ObjectMeta: metav1.ObjectMeta{Name: "pyfer", Namespace: "team"},
		Spec: aeiv1.AgentAppSpec{
			ProviderClassName: className,
			Dispatchers:       []string{"pyfer"},
			Identity:          aeiv1.AgentAppIdentity{Audience: "pyfer-agent"},
		},
	}
	class := &aeiv1.AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: className},
		Spec: aeiv1.AgentProviderClassSpec{
			Provider:              "test-isolation",
			DefaultIsolationClass: defaultIso,
			Capabilities:          caps,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(app, class).Build()
	plane := control.New(control.Config{Secret: []byte("k")})
	h := NewHandler(plane, echoAuth{namespace: "team"}, c)
	mux := http.NewServeMux()
	plane.RegisterRoutes(mux)
	h.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestDispatchAppliesClassIsolation is the data-plane half of the silent-downgrade
// fix: an app names only its class, so the class's defaultIsolationClass is the
// only thing that can isolate its runs — it must reach the provider.
func TestDispatchAppliesClassIsolation(t *testing.T) {
	srv := newIsolationServer(t, "isolated", "gvisor", "ASYNC", "ISOLATION:gvisor")

	resp := do(t, http.MethodPost, srv.URL+"/aei/v1alpha1/dispatch", "pyfer", dispatchRequest{
		App: "pyfer", TaskRef: "item://1", TimeoutSeconds: 30,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	spec := <-isolationLauncher.specs
	if spec.Isolation != "gvisor" {
		t.Fatalf("RunSpec.Isolation = %q, want gvisor — the class default never reached the provider", spec.Isolation)
	}
}

// TestDispatchRejectsUnadvertisedIsolation: the class asks for an isolation class
// it does not advertise, so the dispatch is refused instead of running unisolated.
func TestDispatchRejectsUnadvertisedIsolation(t *testing.T) {
	srv := newIsolationServer(t, "mismatched", "gvisor", "ISOLATION:kata")

	resp := do(t, http.MethodPost, srv.URL+"/aei/v1alpha1/dispatch", "pyfer", dispatchRequest{
		App: "pyfer", TaskRef: "item://1", TimeoutSeconds: 30,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	select {
	case spec := <-isolationLauncher.specs:
		t.Fatalf("dispatched run %q despite unsatisfiable isolation", spec.ID)
	default:
	}
}
