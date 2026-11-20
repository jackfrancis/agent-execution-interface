package conformance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/aeiruntime"
	"github.com/jackfrancis/agent-execution-interface/control"
)

// TestCompletionSurvivesOneDroppedCall: the first /complete call reaching the
// control plane is dropped before it is handled. The provider, like
// agent-substrate, only waits for the deadline (dispatchCapture), so the runtime's
// callback is the one signal that can mark the run succeeded. The runtime SDK
// retries, and the run is recorded as succeeded well before its deadline.
func TestCompletionSurvivesOneDroppedCall(t *testing.T) {
	plane := control.New(control.Config{Secret: []byte("conformance-secret")})
	var completeCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/aei/v1alpha1/complete" && completeCalls.Add(1) == 1 {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				_ = conn.Close() // drop the call, as a controller restart would
			}
			return
		}
		plane.Handler().ServeHTTP(w, r)
	}))
	defer srv.Close()

	launcher := &dispatchCapture{dispatched: make(chan dispatched, 1)}
	const deadline = 30 * time.Second
	runID, err := plane.Submit(context.Background(), launcher, aei.RunSpec{ID: "run-dropped", TaskRef: "task://x", Deadline: deadline})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Act as the runtime: decode the injection and report success.
	d := <-launcher.dispatched
	env := aei.Injection(d.spec, d.cred, srv.URL+"/aei/v1alpha1", "", time.Now())
	rt, err := aeiruntime.Load(context.Background(), getenvFrom(env), nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rt.Complete(ctx, nil, aeiruntime.Timing{RuntimeSeconds: 1}, "res://done"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := completeCalls.Load(); got != 2 {
		t.Errorf("/complete calls = %d, want 2 (one dropped, one delivered)", got)
	}

	// Well inside the deadline, so only the retried callback can explain a success.
	waitUntil := time.Now().Add(5 * time.Second)
	for {
		st, ok := plane.Status(runID)
		if ok && st.Phase != "Running" {
			if st.Phase != "Succeeded" || st.ResultRef != "res://done" {
				t.Fatalf("run state = %+v, want Succeeded with res://done", st)
			}
			return
		}
		if time.Now().After(waitUntil) {
			t.Fatalf("run %s still %q after the retried completion", runID, st.Phase)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestOneShotReportStopsAtControllerDeadline pairs the runtime SDK with the
// control plane's own cutoff: a provider that spends 5s on setup before stamping
// its dispatch time still injects the plane's deadline, so a one-shot runtime whose
// callback keeps failing stops reporting 10s before the deadline the controller
// enforces, not 10s before the later dispatch-plus-budget instant.
func TestOneShotReportStopsAtControllerDeadline(t *testing.T) {
	plane := control.New(control.Config{Secret: []byte("conformance-secret")})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/aei/v1alpha1/complete" {
			w.WriteHeader(http.StatusServiceUnavailable) // a callback outage
			return
		}
		plane.Handler().ServeHTTP(w, r)
	}))
	defer srv.Close()

	launcher := &dispatchCapture{dispatched: make(chan dispatched, 1)}
	const budget = 12 * time.Second // leaves a 2s retry window before the 10s exit margin
	start := time.Now()
	if _, err := plane.Submit(context.Background(), launcher, aei.RunSpec{TaskRef: "task://setup", Deadline: budget}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	d := <-launcher.dispatched
	lateStamp := time.Now().Add(5 * time.Second) // provider setup before it stamps dispatch
	env := aei.Injection(d.spec, d.cred, srv.URL+"/aei/v1alpha1", "", lateStamp)
	rt, err := aeiruntime.Load(context.Background(), getenvFrom(env), nil) // env transport: one-shot
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := rt.Complete(context.Background(), nil, aeiruntime.Timing{}, "res://late"); err == nil {
		t.Fatal("want the outage reported as an error")
	}
	// The window ends at start+2s against the plane's cutoff; against the late stamp
	// it would end at start+7s.
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Errorf("reporting stopped %v after dispatch; want about 2s (the plane's cutoff minus the exit margin)", elapsed)
	}
}
