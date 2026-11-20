package control

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// gatedAsync is an AsyncLauncher whose Dispatch waits for the test to open its
// gate, holding the run's create slot meanwhile; Await then succeeds at once.
type gatedAsync struct {
	entered chan string // receives each run id as its Dispatch starts
	gate    chan struct{}
}

func newGatedAsync() *gatedAsync {
	return &gatedAsync{entered: make(chan string, 8), gate: make(chan struct{})}
}

func (g *gatedAsync) open() { close(g.gate) }

func (g *gatedAsync) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	return g.Dispatch(ctx, spec, cred)
}

func (g *gatedAsync) Dispatch(ctx context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	g.entered <- spec.ID
	select {
	case <-g.gate:
	case <-ctx.Done():
		return aei.Handle{Provider: "gated"}, ctx.Err()
	}
	return aei.Handle{Provider: "gated", Ref: spec.ID}, nil
}

func (g *gatedAsync) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{ResultRef: "ok"}, nil
}

// byTask indexes observations by the run's task ref.
func byTask(obs []RunObservation) map[string]RunObservation {
	out := map[string]RunObservation{}
	for _, o := range obs {
		out[o.TaskRef] = o
	}
	return out
}

// With one create slot, a run submitted while another's Dispatch holds the slot
// waits in the queue; its queue wait covers that hold, and each run's dispatch
// latency covers its own Dispatch.
func TestSubmitObservesQueueWaitAndDispatchLatency(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs, MaxConcurrentCreates: 1})

	const hold = 120 * time.Millisecond
	g := newGatedAsync()
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{TaskRef: "first", Deadline: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	<-g.entered // the first run holds the only create slot
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{TaskRef: "second", Deadline: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(hold)
	g.open()

	got := byTask(obs.waitObserved(t, 2))
	first, second := got["first"], got["second"]
	if !first.DispatchMeasured || first.DispatchLatency < hold {
		t.Errorf("first dispatch latency = %v (measured %v), want at least %v", first.DispatchLatency, first.DispatchMeasured, hold)
	}
	if !second.QueueMeasured || second.QueueWait < hold {
		t.Errorf("second queue wait = %v (measured %v), want at least %v", second.QueueWait, second.QueueMeasured, hold)
	}
	if !first.QueueMeasured {
		t.Error("first queue wait not measured")
	}
	if !second.DispatchMeasured {
		t.Error("second dispatch latency not measured")
	}
	for _, o := range []RunObservation{first, second} {
		if o.QueueWait+o.DispatchLatency > o.Duration {
			t.Errorf("%s: queue %v + dispatch %v exceeds total %v", o.TaskRef, o.QueueWait, o.DispatchLatency, o.Duration)
		}
	}
}

// A run whose deadline passes while it is still queued waited its whole duration
// in the queue and never reached Dispatch.
func TestSubmitObservesRunEndedInQueue(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs, MaxConcurrentCreates: 1})

	g := newGatedAsync()
	var once sync.Once
	defer once.Do(g.open)
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{TaskRef: "holder", Deadline: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	<-g.entered

	const deadline = 100 * time.Millisecond
	id, err := p.Submit(context.Background(), g, aei.RunSpec{TaskRef: "queued", Deadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, id)
	o := byTask(obs.waitObserved(t, 1))["queued"]
	if o.Phase != phaseFailed {
		t.Errorf("phase = %q, want Failed", o.Phase)
	}
	if !o.QueueMeasured || o.QueueWait != o.Duration || o.QueueWait < deadline {
		t.Errorf("queue wait = %v (measured %v), want the whole duration %v, at least %v", o.QueueWait, o.QueueMeasured, o.Duration, deadline)
	}
	if o.DispatchMeasured {
		t.Errorf("dispatch latency %v measured for a run that never reached Dispatch", o.DispatchLatency)
	}
	once.Do(g.open)
}

// A plain provider's Launch is the whole run, so it has a queue wait but no
// dispatch latency.
func TestSubmitSyncProviderHasNoDispatchLatency(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs})
	id, err := p.Submit(context.Background(), fakeSync{}, aei.RunSpec{TaskRef: "t"})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, id)
	o := obs.waitObserved(t, 1)[0]
	if !o.QueueMeasured || o.DispatchMeasured {
		t.Errorf("observed queue measured %v, dispatch measured %v; want true, false", o.QueueMeasured, o.DispatchMeasured)
	}
}

// slowStore delays the writes the Submit path makes around a run, to reproduce
// the races between a run's outcome and its bookkeeping.
type slowStore struct {
	*memStore
	setRunningDelay time.Duration
	finishEntered   chan struct{} // closed when the first FinishRun starts
	finishRelease   chan struct{} // the first FinishRun waits for this
	finishOnce      atomic.Bool
}

func (s *slowStore) SetRunning(runID string) {
	time.Sleep(s.setRunningDelay)
	s.memStore.SetRunning(runID)
}

func (s *slowStore) FinishRun(runID string, st RunState) bool {
	if s.finishRelease != nil && s.finishOnce.CompareAndSwap(false, true) {
		close(s.finishEntered)
		<-s.finishRelease
	}
	return s.memStore.FinishRun(runID, st)
}

// lateDispatch is an AsyncLauncher whose Dispatch ignores cancellation and returns
// only when the test lets it, after the run's deadline has already won.
type lateDispatch struct {
	proceed  chan struct{}
	returned chan struct{}
}

func (l *lateDispatch) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	return l.Dispatch(ctx, spec, cred)
}

func (l *lateDispatch) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	<-l.proceed
	defer close(l.returned)
	return aei.Handle{Provider: "late", Ref: spec.ID}, nil
}

func (l *lateDispatch) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{}, nil
}

// The stage times freeze when the run's outcome is decided. A Dispatch that
// returns while the terminal store write is still in progress (after the deadline
// won) is not counted as measured, and the run's duration ends before the write.
func TestStageTimesFreezeWhenOutcomeIsDecided(t *testing.T) {
	store := &slowStore{memStore: newMemStore(), finishEntered: make(chan struct{}), finishRelease: make(chan struct{})}
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs, Store: store})
	l := &lateDispatch{proceed: make(chan struct{}), returned: make(chan struct{})}

	before := time.Now()
	if _, err := p.Submit(context.Background(), l, aei.RunSpec{TaskRef: "t", Deadline: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	<-store.finishEntered // the deadline won; the terminal write is in progress
	entered := time.Now()
	close(l.proceed) // Dispatch returns now, after the outcome was decided
	<-l.returned
	time.Sleep(20 * time.Millisecond) // the write is still slow
	close(store.finishRelease)

	o := obs.waitObserved(t, 1)[0]
	if o.DispatchMeasured {
		t.Errorf("dispatch latency %v measured for a Dispatch that returned after the run ended", o.DispatchLatency)
	}
	if limit := entered.Sub(before); o.Duration > limit {
		t.Errorf("duration %v includes the terminal store write (it began %v after Submit)", o.Duration, limit)
	}
	if o.QueueWait > o.Duration {
		t.Errorf("queue wait %v exceeds duration %v", o.QueueWait, o.Duration)
	}
}

// A run is queued from admission. When the only create slot is taken and Submit's
// own store write outlasts the deadline, the run can end before its goroutine is
// scheduled; it still records its whole duration as queue wait.
func TestQueueWaitCountsFromAdmission(t *testing.T) {
	const deadline = 30 * time.Millisecond
	store := &slowStore{memStore: newMemStore()}
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs, Store: store, MaxConcurrentCreates: 1})

	g := newGatedAsync()
	defer g.open()
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{TaskRef: "holder", Deadline: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	<-g.entered // the only create slot is held

	store.setRunningDelay = 3 * deadline // SetRunning outlasts the next run's deadline
	id, err := p.Submit(context.Background(), g, aei.RunSpec{TaskRef: "queued", Deadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, id)
	o := byTask(obs.waitObserved(t, 1))["queued"]
	if !o.QueueMeasured || o.QueueWait != o.Duration || o.QueueWait < deadline {
		t.Errorf("queue wait = %v (measured %v), want the whole duration %v, at least %v", o.QueueWait, o.QueueMeasured, o.Duration, deadline)
	}
	if o.DispatchMeasured {
		t.Error("dispatch measured for a run that never left the queue")
	}
}
