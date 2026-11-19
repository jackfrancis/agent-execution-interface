package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// quietLog keeps the expected "provider panicked" stack traces out of test output.
var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// panicAsync is an AsyncLauncher that panics in the named method.
type panicAsync struct{ in string }

func (f panicAsync) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	panic("Launch must not be called on an async provider")
}
func (f panicAsync) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	if f.in == "Dispatch" {
		var m map[string]int
		m["nil map"]++ // a real provider bug, not just panic("...")
	}
	return aei.Handle{Ref: spec.ID}, nil
}
func (f panicAsync) Await(context.Context, aei.Handle) (aei.Result, error) {
	if f.in == "Await" {
		panic("await exploded")
	}
	return aei.Result{ResultRef: "unreachable"}, nil
}

// panicSync is a plain (blocking) Launcher that panics in Launch.
type panicSync struct{}

func (panicSync) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	panic("launch exploded")
}

// panicPull panics while the plane asks how it takes its credential (during mint),
// outside any provider create or watch.
type panicPull struct{ fakeWatch }

func (*panicPull) PullsCredential() bool { panic("pulls exploded") }

// awaitErr is an AsyncLauncher whose watch fails: Await returns an error and a
// zero Result, which the plane used to record as success.
type awaitErr struct{}

func (awaitErr) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (awaitErr) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	return aei.Handle{Ref: spec.ID}, nil
}
func (awaitErr) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{Timing: aei.Timing{RuntimeSeconds: 3}}, errors.New("watch lost the workload")
}

// gated is an AsyncLauncher whose Dispatch blocks until gate is closed (or, with a
// nil gate, sleeps briefly), recording the peak number of concurrent creates.
type gated struct {
	gate     chan struct{}
	entered  chan string // receives each run id as its Dispatch starts (optional)
	inflight atomic.Int32
	peak     atomic.Int32
}

func (g *gated) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (g *gated) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	n := g.inflight.Add(1)
	defer g.inflight.Add(-1)
	for {
		old := g.peak.Load()
		if n <= old || g.peak.CompareAndSwap(old, n) {
			break
		}
	}
	if g.entered != nil {
		g.entered <- spec.ID
	}
	if g.gate != nil {
		<-g.gate
	} else {
		time.Sleep(5 * time.Millisecond)
	}
	return aei.Handle{Ref: spec.ID}, nil
}
func (g *gated) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{ResultRef: "ok"}, nil
}

func mustTerminal(t *testing.T, p *Plane, runID string) RunState {
	t.Helper()
	waitTerminal(t, p, runID)
	st, _ := p.Status(runID)
	return st
}

// A provider panic in Dispatch, Await, or a blocking Launch is recorded as a failed
// run, and the plane then completes the next healthy run. MaxConcurrentCreates is
// 1, so the healthy run also proves the panicking create gave its slot back.
func TestProviderPanicFailsRunAndPlaneContinues(t *testing.T) {
	cases := []struct {
		name     string
		launcher aei.Launcher
		in       string
	}{
		{"dispatch", panicAsync{in: "Dispatch"}, "Dispatch"},
		{"await", panicAsync{in: "Await"}, "Await"},
		{"launch", panicSync{}, "Launch"},
		{"mint", &panicPull{}, "run"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/submit", func(t *testing.T) {
			obs := &captureObserver{}
			p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs, MaxConcurrentCreates: 1, MaxQueued: 1})

			id, err := p.Submit(context.Background(), tc.launcher, aei.RunSpec{ID: "bad", Deadline: 2 * time.Second})
			if tc.in == "run" {
				// A panic during mint happens before the run is accepted, on the
				// caller's goroutine (net/http recovers it for the dispatch API).
				// What matters is that its admission slot is returned.
				if err == nil {
					t.Fatalf("Submit = %q, nil; want the mint panic to surface", id)
				}
			} else {
				if err != nil {
					t.Fatalf("Submit: %v", err)
				}
				st := mustTerminal(t, p, id)
				want := "provider panicked in " + tc.in
				if st.Phase != phaseFailed || !strings.Contains(st.Err, want) {
					t.Fatalf("panicking run = %+v, want Failed containing %q", st, want)
				}
			}

			// The next healthy run completes on the same plane.
			good, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{ID: "good", Deadline: 2 * time.Second})
			if err != nil {
				t.Fatalf("healthy Submit after panic: %v", err)
			}
			if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded || st.ResultRef != "fine" {
				t.Fatalf("healthy run = %+v, want Succeeded/fine", st)
			}
		})

		t.Run(tc.name+"/run", func(t *testing.T) {
			p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1})
			r, _ := p.Run(context.Background(), tc.launcher, aei.RunSpec{ID: "bad", Deadline: 2 * time.Second})
			want := "provider panicked in " + tc.in
			if r.Err == nil || !strings.Contains(r.Err.Error(), want) {
				t.Fatalf("Run result err = %v, want %q", r.Err, want)
			}
			r, err := p.Run(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{ID: "good", Deadline: 2 * time.Second})
			if err != nil || r.Err != nil || r.ResultRef != "fine" {
				t.Fatalf("healthy Run after panic = %+v, %v; want success", r, err)
			}
		})
	}
}

// A provider whose Await returns an error marks its run Failed — on the Submit
// path (status and metrics) and the blocking Run path — instead of Succeeded.
func TestAwaitErrorFailsRun(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs})

	id, err := p.Submit(context.Background(), awaitErr{}, aei.RunSpec{TaskRef: "github-ingest", Deadline: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	st := mustTerminal(t, p, id)
	if st.Phase != phaseFailed || !strings.Contains(st.Err, "watch lost the workload") {
		t.Fatalf("status = %+v, want Failed with the watch error", st)
	}
	if st.Timing.RuntimeSeconds != 3 {
		t.Errorf("timing = %+v, want the provider's timing kept", st.Timing)
	}
	if got := obs.snapshot(); len(got) != 1 || got[0].phase != phaseFailed {
		t.Fatalf("observed %+v, want one Failed observation", got)
	}

	r, _ := p.Run(context.Background(), awaitErr{}, aei.RunSpec{Deadline: 2 * time.Second})
	if r.Err == nil || !strings.Contains(r.Err.Error(), "watch lost the workload") {
		t.Fatalf("Run result err = %v, want the watch error", r.Err)
	}
}

// A burst beyond MaxConcurrentCreates+MaxQueued is refused at once with
// ErrOverloaded and leaves no run state; the queued runs still complete, and the
// plane accepts work again once they have.
func TestSubmitBurstGetsOverloaded(t *testing.T) {
	g := &gated{gate: make(chan struct{}), entered: make(chan string, 8)}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, MaxQueued: 2})

	var accepted []string
	for i := range 3 { // 1 creating + 2 queued
		id, err := p.Submit(context.Background(), g, aei.RunSpec{ID: fmt.Sprintf("r%d", i), Deadline: 5 * time.Second})
		if err != nil {
			t.Fatalf("Submit %d: %v (want accepted within the bound)", i, err)
		}
		accepted = append(accepted, id)
	}
	<-g.entered // one create is in progress; the other two wait for its slot

	start := time.Now()
	_, err := p.Submit(context.Background(), g, aei.RunSpec{ID: "over", Deadline: 5 * time.Second})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Submit beyond the bound = %v, want ErrOverloaded", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Errorf("overloaded Submit took %v; it must not block", time.Since(start))
	}
	if _, ok := p.Status("over"); ok {
		t.Error("a refused submission must leave no run state")
	}

	close(g.gate)
	for _, id := range accepted {
		if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded {
			t.Fatalf("queued run %s = %+v, want Succeeded", id, st)
		}
	}
	if g.peak.Load() != 1 {
		t.Errorf("peak concurrent creates = %d, want 1", g.peak.Load())
	}
	// Admission slots come back once the creates return.
	waitFor(t, func() bool { return len(p.admit) == 0 })
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{Deadline: 5 * time.Second}); err != nil {
		t.Fatalf("Submit after the burst drained: %v", err)
	}
}

// Provider creates never exceed MaxConcurrentCreates, across Submit and Run, and
// a create slot is not held while a run awaits completion.
func TestConcurrentCreatesBounded(t *testing.T) {
	g := &gated{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 2, MaxQueued: 64})

	var ids []string
	for range 20 {
		id, err := p.Submit(context.Background(), g, aei.RunSpec{Deadline: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if r, _ := p.Run(context.Background(), g, aei.RunSpec{Deadline: 5 * time.Second}); r.Err != nil {
				t.Errorf("Run: %v", r.Err)
			}
		})
	}
	wg.Wait()
	for _, id := range ids {
		if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded {
			t.Fatalf("run %s = %+v, want Succeeded", id, st)
		}
	}
	if peak := g.peak.Load(); peak != 2 {
		t.Errorf("peak concurrent creates = %d, want exactly 2", peak)
	}
}

// Run shares the create bound but has no queue cap: it waits for a slot, and if
// none frees up before its deadline it fails as undispatched.
func TestRunWaitsForCreateSlotUntilDeadline(t *testing.T) {
	g := &gated{gate: make(chan struct{}), entered: make(chan string, 1)}
	defer close(g.gate)
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1})

	if _, err := p.Submit(context.Background(), g, aei.RunSpec{Deadline: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	<-g.entered // the only create slot is held

	r, err := p.Run(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "late"}}, aei.RunSpec{Deadline: 50 * time.Millisecond})
	if err == nil || r.Err == nil || !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Fatalf("Run = %+v, %v; want a deadline failure waiting for a slot", r, err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}
