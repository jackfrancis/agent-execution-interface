package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
	return newTestServerWith(t, control.Config{Secret: []byte("k")})
}

// newTestServerWith is newTestServer with a caller-supplied plane config.
func newTestServerWith(t *testing.T, cfg control.Config) *httptest.Server {
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
	plane := control.New(cfg)
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

// TestDispatchWithoutTimeoutEndsAtDefault: a dispatch that omits timeoutSeconds
// still ends — the plane applies its default deadline, and with no callback and a
// provider that never reports on its own, the run reads Failed with the deadline
// error once the default passes.
func TestDispatchWithoutTimeoutEndsAtDefault(t *testing.T) {
	const def = 100 * time.Millisecond
	srv := newTestServerWith(t, control.Config{Secret: []byte("k"), DefaultDeadline: def})

	start := time.Now()
	resp := do(t, http.MethodPost, srv.URL+"/aei/v1alpha1/dispatch", "pyfer", map[string]any{
		"app":      "pyfer",
		"taskRef":  "item://no-timeout",
		"identity": map[string]any{"subject": "github:1", "scopes": []string{"signals:read"}},
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("dispatch = %d, want 202", resp.StatusCode)
	}
	var out struct {
		RunID string `json:"runId"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	<-testLauncher.dispatched // drain: this run never calls back

	end := time.Now().Add(2 * time.Second)
	for {
		s := do(t, http.MethodGet, srv.URL+"/aei/v1alpha1/runs/"+out.RunID, "pyfer", nil)
		var st struct{ Phase, Message string }
		_ = json.NewDecoder(s.Body).Decode(&st)
		if st.Phase != "Running" && st.Phase != "" {
			if elapsed := time.Since(start); elapsed < def {
				t.Errorf("run ended after %s, before the %s default", elapsed, def)
			}
			if st.Phase != "Failed" || !strings.Contains(st.Message, control.ErrDeadlineExceeded.Error()) {
				t.Errorf("status = %q %q, want Failed with %q", st.Phase, st.Message, control.ErrDeadlineExceeded.Error())
			}
			return
		}
		if time.Now().After(end) {
			t.Fatalf("run without a timeout never ended (last phase %q)", st.Phase)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDispatchOversizedTimeoutIsCapped: a timeoutSeconds too large for a
// time.Duration saturates and is capped at the plane's maximum, rather than
// wrapping to a deadline of a few hundred milliseconds.
func TestDispatchOversizedTimeoutIsCapped(t *testing.T) {
	srv := newTestServer(t) // default maximum: 1h
	resp := do(t, http.MethodPost, srv.URL+"/aei/v1alpha1/dispatch", "pyfer", map[string]any{
		"app":            "pyfer",
		"taskRef":        "item://huge-timeout",
		"timeoutSeconds": int64(18_446_744_074), // * time.Second wraps to ~290ms
		"identity":       map[string]any{"subject": "github:1", "scopes": []string{"signals:read"}},
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("dispatch = %d, want 202", resp.StatusCode)
	}
	var out struct {
		RunID string `json:"runId"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	<-testLauncher.dispatched // drain: this run never calls back

	time.Sleep(600 * time.Millisecond) // well past the wrapped ~290ms
	s := do(t, http.MethodGet, srv.URL+"/aei/v1alpha1/runs/"+out.RunID, "pyfer", nil)
	var st struct{ Phase, Message string }
	_ = json.NewDecoder(s.Body).Decode(&st)
	if st.Phase != "Running" {
		t.Errorf("status = %q %q, want Running under the 1h maximum", st.Phase, st.Message)
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

// The cached launcher keeps the driver it was built from, so if an admin edits the
// class to another driver, runs the cached launcher still dispatches are credited
// to the driver that actually runs them, not to the class's new one.
func TestLauncherCacheKeepsItsDriver(t *testing.T) {
	h := NewHandler(control.New(control.Config{Secret: []byte("k")}), echoAuth{namespace: "team"}, nil)
	class := &aeiv1.AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "standard"},
		Spec:       aeiv1.AgentProviderClassSpec{Provider: "test-dispatch"},
	}
	first, driver, err := h.launcherForClass(class)
	if err != nil || driver != "test-dispatch" {
		t.Fatalf("launcherForClass = %q, %v; want test-dispatch", driver, err)
	}

	class.Spec.Provider = "k8s-job" // the admin edits the class
	again, driver, err := h.launcherForClass(class)
	if err != nil {
		t.Fatal(err)
	}
	if again != first || driver != "test-dispatch" {
		t.Errorf("cached launcher driver = %q (same launcher %v), want the cached test-dispatch", driver, again == first)
	}
}

// slowAuth authenticates like echoAuth but only after delay, ignoring its context,
// as a slow TokenReview or class lookup would.
type slowAuth struct {
	echoAuth
	delay time.Duration
}

func (s slowAuth) Authenticate(ctx context.Context, bearer string) (Caller, error) {
	time.Sleep(s.delay)
	return s.echoAuth.Authenticate(ctx, bearer)
}

// TestDispatchRefusesAfterRequestDeadline: when the lookups before Submit outlast
// the request's deadline, dispatch answers 503 and launches nothing, so a caller
// that retries cannot end up with two runs.
func TestDispatchRefusesAfterRequestDeadline(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aeiv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	app := &aeiv1.AgentApp{
		ObjectMeta: metav1.ObjectMeta{Name: "pyfer", Namespace: "team"},
		Spec:       aeiv1.AgentAppSpec{ProviderClassName: "standard", Dispatchers: []string{"pyfer"}},
	}
	class := &aeiv1.AgentProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "standard"},
		Spec:       aeiv1.AgentProviderClassSpec{Provider: "test-dispatch"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(app, class).Build()
	h := NewHandler(control.New(control.Config{Secret: []byte("k")}), slowAuth{echoAuth{namespace: "team"}, 100 * time.Millisecond}, c)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	b, _ := json.Marshal(dispatchRequest{App: "pyfer", TaskRef: "item://late", TimeoutSeconds: 30})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/aei/v1alpha1/dispatch", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer pyfer")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("dispatch = %d, want 503", rec.Code)
	}
	select {
	case <-testLauncher.dispatched:
		t.Fatal("a run was launched after the request deadline")
	case <-time.After(100 * time.Millisecond):
	}
}
