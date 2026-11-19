package conformance

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/aeiruntime"
	"github.com/jackfrancis/agent-execution-interface/control"
)

// TestTimingPresenceRoundTrip runs the Go runtime SDK against the real control
// plane: an empty Timing reports no durations, and a duration the runtime marks
// measured reaches the observer as measured even when it is zero
// (docs/runtime-abi.md §3).
func TestTimingPresenceRoundTrip(t *testing.T) {
	obs := &timingObserver{}
	plane := control.New(control.Config{Secret: []byte("k"), Observer: obs})
	srv := httptest.NewServer(plane.Handler())
	defer srv.Close()

	for _, tc := range []struct {
		name string
		sent aeiruntime.Timing
		want aei.TimingStages
	}{
		{"empty report", aeiruntime.Timing{}, 0},
		{"measured zero model time", aeiruntime.Timing{RuntimeSeconds: 2, ModelCalls: 1, Measured: aeiruntime.StageModel},
			aei.StageRuntime | aei.StageModel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(obs.snapshot())
			l := &dispatchCapture{dispatched: make(chan dispatched, 1)}
			if _, err := plane.Submit(context.Background(), l, aei.RunSpec{TaskRef: "t", Deadline: 5 * time.Second}); err != nil {
				t.Fatal(err)
			}
			d := <-l.dispatched
			env := aei.Injection(d.spec, d.cred, srv.URL+"/aei/v1alpha1", "", time.Now())
			rt, err := aeiruntime.Load(context.Background(), getenvFrom(env), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := rt.Complete(context.Background(), nil, tc.sent, ""); err != nil {
				t.Fatal(err)
			}
			got := obs.wait(t, before+1)[before].Timing
			if got.Measured != tc.want {
				t.Errorf("Measured = %03b, want %03b (timing %+v)", got.Measured, tc.want, got)
			}
		})
	}
}

type dispatched struct {
	spec aei.RunSpec
	cred aei.Credential
}

// dispatchCapture is an AsyncLauncher that hands the test the dispatched spec and
// credential, so the test can act as the runtime; its Await waits for the deadline.
type dispatchCapture struct{ dispatched chan dispatched }

func (d *dispatchCapture) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, _ := d.Dispatch(ctx, spec, cred)
	return h, nil
}

func (d *dispatchCapture) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	d.dispatched <- dispatched{spec, cred}
	return aei.Handle{Provider: "capture", Ref: spec.ID}, nil
}

func (d *dispatchCapture) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
}

type timingObserver struct {
	mu   sync.Mutex
	runs []control.RunObservation
}

func (o *timingObserver) ObserveRun(r control.RunObservation) {
	o.mu.Lock()
	o.runs = append(o.runs, r)
	o.mu.Unlock()
}

func (o *timingObserver) snapshot() []control.RunObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]control.RunObservation(nil), o.runs...)
}

func (o *timingObserver) wait(t *testing.T, n int) []control.RunObservation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := o.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d observations, got %d", n, len(o.snapshot()))
	return nil
}
