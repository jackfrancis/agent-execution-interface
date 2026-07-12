package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
	aeiv1 "github.com/jackfrancis/agent-execution-interface/controller/api/v1alpha1"
)

// captureLauncher is an AsyncLauncher whose Await blocks until the deadline, so
// completion must arrive via the runtime's /complete callback. It captures the
// pushed run credential so the test can act as the runtime.
type captureLauncher struct{ dispatched chan aei.Credential }

func (c *captureLauncher) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, _ := c.Dispatch(ctx, spec, cred)
	r, _ := c.Await(ctx, h)
	return h, r.Err
}
func (c *captureLauncher) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	c.dispatched <- cred
	return aei.Handle{Provider: "test-dispatch", Ref: spec.ID}, nil
}
func (c *captureLauncher) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
}

var testLauncher = &captureLauncher{dispatched: make(chan aei.Credential, 8)}

func init() {
	aei.Register("test-dispatch", func(aei.Config) (aei.Launcher, error) { return testLauncher, nil })
}

// echoAuth authenticates any non-empty token as a ServiceAccount named by the
// token itself, in a fixed namespace — so a test picks the caller identity by
// choosing the bearer string.
type echoAuth struct{ namespace string }

func (e echoAuth) Authenticate(_ context.Context, bearer string) (Caller, error) {
	if bearer == "" {
		return Caller{}, errors.New("no token")
	}
	return Caller{Namespace: e.namespace, ServiceAccount: bearer}, nil
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := aeiv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	app := &aeiv1.AgentApp{
		ObjectMeta: metav1.ObjectMeta{Name: "pyfer", Namespace: "team"},
		Spec: aeiv1.AgentAppSpec{
			ProviderClassName: "standard",
			Dispatchers:       []string{"pyfer"},
			Identity:          aeiv1.AgentAppIdentity{Audience: "pyfer-agent", MaxScopes: []string{"signals:read", "metadata:write"}},
		},
	}
	class := &aeiv1.AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "standard"},
		Spec:       aeiv1.AgentProviderClassSpec{Provider: "test-dispatch"},
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

func do(t *testing.T, method, url, bearer string, body any) *http.Response {
	t.Helper()
	var r *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	} else {
		r = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, url, r)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// TestDispatchCompleteStatus is the end-to-end data-plane happy path: submit a
// run (no CR), complete it as the runtime, and read the terminal status back.
func TestDispatchCompleteStatus(t *testing.T) {
	srv := newTestServer(t)

	resp := do(t, http.MethodPost, srv.URL+"/aei/v1alpha1/dispatch", "pyfer", dispatchRequest{
		App:            "pyfer",
		TaskRef:        "item://1",
		Parameters:     map[string]string{"kind": "converse"},
		TimeoutSeconds: 30,
		Identity: struct {
			Subject string   `json:"subject"`
			Scopes  []string `json:"scopes"`
		}{Subject: "github:1", Scopes: []string{"signals:read"}},
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("dispatch = %d, want 202", resp.StatusCode)
	}
	var out struct {
		RunID string `json:"runId"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.RunID == "" {
		t.Fatal("dispatch returned no runId")
	}

	// Act as the runtime: the pushed credential authorizes /complete.
	cred := <-testLauncher.dispatched
	c := do(t, http.MethodPost, srv.URL+"/aei/v1alpha1/complete", cred.Value,
		map[string]any{"run_id": out.RunID, "result_ref": "res://1"})
	if c.StatusCode != http.StatusAccepted {
		t.Fatalf("complete = %d, want 202", c.StatusCode)
	}

	// Status eventually reports Succeeded (finishRun runs just after the callback).
	deadline := time.Now().Add(2 * time.Second)
	for {
		s := do(t, http.MethodGet, srv.URL+"/aei/v1alpha1/runs/"+out.RunID, "pyfer", nil)
		var st struct{ Phase, ResultRef string }
		_ = json.NewDecoder(s.Body).Decode(&st)
		if st.Phase == "Succeeded" {
			if st.ResultRef != "res://1" {
				t.Errorf("resultRef = %q, want res://1", st.ResultRef)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run never reached Succeeded (last phase %q)", st.Phase)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDispatchAuthorization(t *testing.T) {
	srv := newTestServer(t)
	base := srv.URL + "/aei/v1alpha1/dispatch"
	ok := struct {
		Subject string   `json:"subject"`
		Scopes  []string `json:"scopes"`
	}{Subject: "github:1", Scopes: []string{"signals:read"}}

	cases := []struct {
		name   string
		bearer string
		body   dispatchRequest
		want   int
	}{
		{"no token", "", dispatchRequest{App: "pyfer"}, http.StatusUnauthorized},
		{"unknown app", "pyfer", dispatchRequest{App: "ghost", Identity: ok}, http.StatusNotFound},
		{"caller not a dispatcher", "intruder", dispatchRequest{App: "pyfer", Identity: ok}, http.StatusForbidden},
		{"scope exceeds policy", "pyfer", dispatchRequest{App: "pyfer", Identity: struct {
			Subject string   `json:"subject"`
			Scopes  []string `json:"scopes"`
		}{Subject: "github:1", Scopes: []string{"admin:everything"}}}, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := do(t, http.MethodPost, base, tc.bearer, tc.body)
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}
