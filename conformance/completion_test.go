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

// detachedLauncher behaves like the agent-substrate provider: Dispatch hands the
// run off and Await only waits for the deadline, so the runtime's /complete call is
// the one signal that can mark the run succeeded.
type detachedLauncher struct {
	dispatched chan dispatched
}

type dispatched struct {
	spec aei.RunSpec
	cred aei.Credential
}

func (l *detachedLauncher) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, err := l.Dispatch(ctx, spec, cred)
	if err != nil {
		return h, err
	}
	r, _ := l.Await(ctx, h)
	return h, r.Err
}

func (l *detachedLauncher) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	l.dispatched <- dispatched{spec: spec, cred: cred}
	return aei.Handle{Provider: "detached", Ref: spec.ID}, nil
}

func (l *detachedLauncher) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
}

// TestCompletionSurvivesOneDroppedCall: the first /complete call reaching the
// control plane is dropped before it is handled. The runtime SDK retries, and the
// run is recorded as succeeded well before its deadline backstop would end it.
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

	launcher := &detachedLauncher{dispatched: make(chan dispatched, 1)}
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
