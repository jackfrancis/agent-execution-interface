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
// before any provider create or watch.
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

	mu        sync.Mutex
	deadlines map[string]time.Duration // spec.Deadline each Dispatch was handed
	creds     map[string]aei.Credential
}

func (g *gated) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (g *gated) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	n := g.inflight.Add(1)
	defer g.inflight.Add(-1)
	for {
		old := g.peak.Load()
		if n <= old || g.peak.CompareAndSwap(old, n) {
			break
		}
	}
	g.mu.Lock()
	if g.deadlines == nil {
		g.deadlines, g.creds = map[string]time.Duration{}, map[string]aei.Credential{}
	}
	g.deadlines[spec.ID], g.creds[spec.ID] = spec.Deadline, cred
	g.mu.Unlock()
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

// A provider panic in Dispatch, Await, a blocking Launch, or PullsCredential
// (during mint) is recorded as a failed run, and the plane then completes the next healthy run. MaxConcurrentCreates is
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
		{"mint", &panicPull{}, "mint"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/submit", func(t *testing.T) {
			obs := &captureObserver{}
			p := New(Config{Secret: []byte("k"), Logger: quietLog, Observer: obs, MaxConcurrentCreates: 1, MaxQueued: 1})

			id, err := p.Submit(context.Background(), tc.launcher, aei.RunSpec{ID: "bad", Deadline: 2 * time.Second})
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
			st := mustTerminal(t, p, id)
			want := "provider panicked in " + tc.in
			if st.Phase != phaseFailed || !strings.Contains(st.Err, want) {
				t.Fatalf("panicking run = %+v, want Failed containing %q", st, want)
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

// gatedPull is a gated provider that pulls its credential (a redemption ticket).
type gatedPull struct{ *gated }

func (gatedPull) PullsCredential() bool { return true }

// hangDispatch is an AsyncLauncher whose Dispatch never finishes on its own: it
// returns only when its context ends, like substrate retrying a 503 actor.
type hangDispatch struct{}

func (hangDispatch) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (hangDispatch) Dispatch(ctx context.Context, _ aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	<-ctx.Done()
	return aei.Handle{}, fmt.Errorf("actor did not become ready: %w", ctx.Err())
}
func (hangDispatch) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{}, nil
}

// slowLaunch is a plain Launcher whose Launch (the whole run) blocks until release.
type slowLaunch struct{ entered, release chan struct{} }

func (s slowLaunch) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	close(s.entered)
	<-s.release
	return aei.Handle{}, nil
}

// A create that hangs holds its slot for at most CreateTimeout, even for a run
// with no deadline: the hung Dispatch is cancelled and its run fails, and healthy
// runs behind it complete.
func TestHungCreateGivesUpSlot(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 2, MaxQueued: 4, CreateTimeout: 50 * time.Millisecond})

	var hung []string
	for range 2 { // occupy both slots, with no run deadline
		id, err := p.Submit(context.Background(), hangDispatch{}, aei.RunSpec{})
		if err != nil {
			t.Fatal(err)
		}
		hung = append(hung, id)
	}
	good, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded {
		t.Fatalf("healthy run behind hung creates = %+v, want Succeeded", st)
	}
	for _, id := range hung {
		st := mustTerminal(t, p, id)
		if st.Phase != phaseFailed || !strings.Contains(st.Err, "provider create did not finish within 50ms") {
			t.Fatalf("hung run = %+v, want Failed at the create timeout", st)
		}
	}
}

// A plain provider's Launch is the run itself and gives no signal for when its
// workload exists, so it holds its create slot until it returns, past
// CreateTimeout: no second Launch starts while the first is outstanding.
func TestBlockingLaunchHoldsSlotUntilReturn(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 20 * time.Millisecond})
	slow := slowLaunch{entered: make(chan struct{}), release: make(chan struct{})}
	id, err := p.Submit(context.Background(), slow, aei.RunSpec{Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	<-slow.entered // the long Launch holds the only slot

	next := slowLaunch{entered: make(chan struct{}), release: make(chan struct{})}
	close(next.release)
	nextID, err := p.Submit(context.Background(), next, aei.RunSpec{Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-next.entered:
		t.Fatal("a second Launch started while the first was still running (past the create timeout)")
	case <-time.After(200 * time.Millisecond):
	}
	close(slow.release)
	for _, r := range []string{id, nextID} {
		if st := mustTerminal(t, p, r); st.Phase != phaseSucceeded {
			t.Fatalf("run %s = %+v, want Succeeded", r, st)
		}
	}
}

// Admission is released with the create slot, not when the run's outcome is
// known: a callback that wins while a blocking Launch still holds its slot does
// not open the queue. With one slot and one queue place, one more Submit fits.
func TestAdmissionHeldUntilCreateReturns(t *testing.T) {
	for _, path := range []string{"submit", "run"} {
		t.Run(path, func(t *testing.T) {
			p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, MaxQueued: 1})
			slow := slowLaunch{entered: make(chan struct{}), release: make(chan struct{})}
			defer close(slow.release)
			spec := aei.RunSpec{ID: "first", Deadline: 5 * time.Second}

			runDone := make(chan struct{})
			if path == "submit" {
				if _, err := p.Submit(context.Background(), slow, spec); err != nil {
					t.Fatal(err)
				}
			} else {
				go func() { _, _ = p.Run(context.Background(), slow, spec); close(runDone) }()
			}
			<-slow.entered
			p.fire("first", aei.Result{ResultRef: "called-back"}) // the runtime's callback wins
			if path == "submit" {
				if st := mustTerminal(t, p, "first"); st.Phase != phaseSucceeded {
					t.Fatalf("first = %+v, want Succeeded by callback", st)
				}
			} else {
				<-runDone
			}

			queued := &fakeWatch{result: aei.Result{ResultRef: "ok"}}
			if _, err := p.Submit(context.Background(), queued, aei.RunSpec{Deadline: 5 * time.Second}); err != nil {
				t.Fatalf("Submit into the one queue place: %v", err)
			}
			if _, err := p.Submit(context.Background(), queued, aei.RunSpec{Deadline: 5 * time.Second}); !errors.Is(err, ErrOverloaded) {
				t.Fatalf("Submit beyond the queue while the Launch still creates = %v, want ErrOverloaded", err)
			}
		})
	}
}

// blockPull is a provider whose PullsCredential blocks, standing in for any
// credential step (PullsCredential, the authority, the store) that hangs.
type blockPull struct {
	fakeWatch
	entered, block chan struct{}
}

func (b *blockPull) PullsCredential() bool { close(b.entered); <-b.block; return true }

// Minting counts against CreateTimeout: a credential step that hangs fails its run
// at the timeout and gives the slot to the next create.
func TestHungMintGivesUpSlot(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 50 * time.Millisecond})
	hung := &blockPull{entered: make(chan struct{}), block: make(chan struct{})}
	defer close(hung.block)

	id, err := p.Submit(context.Background(), hung, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	<-hung.entered // the hung mint holds the only slot
	good, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded {
		t.Fatalf("healthy run behind a hung mint = %+v, want Succeeded", st)
	}
	if st := mustTerminal(t, p, id); st.Phase != phaseFailed || !strings.Contains(st.Err, "run credential not ready") {
		t.Fatalf("hung-mint run = %+v, want Failed at the create timeout", st)
	}
}

// The queue limit holds across both paths: an AgentRun (Run) occupying the only
// create slot counts, so with MaxQueued 1 the second Submit is refused.
func TestRunCountsAgainstSubmitQueue(t *testing.T) {
	g := &gated{gate: make(chan struct{}), entered: make(chan string, 4)}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, MaxQueued: 1})

	runDone := make(chan aei.Result, 1)
	go func() {
		r, _ := p.Run(context.Background(), g, aei.RunSpec{ID: "agentrun", Deadline: 5 * time.Second})
		runDone <- r
	}()
	<-g.entered // the AgentRun holds the create slot

	first, err := p.Submit(context.Background(), g, aei.RunSpec{Deadline: 5 * time.Second})
	if err != nil {
		t.Fatalf("first Submit (fills the queue): %v", err)
	}
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{Deadline: 5 * time.Second}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("second Submit = %v, want ErrOverloaded", err)
	}
	close(g.gate)
	if r := <-runDone; r.Err != nil {
		t.Fatalf("AgentRun: %v", r.Err)
	}
	if st := mustTerminal(t, p, first); st.Phase != phaseSucceeded {
		t.Fatalf("queued Submit = %+v, want Succeeded", st)
	}
}

// The credential is minted when the create starts, not when the run is queued: a
// pull ticket handed to a run that waited longer than the ticket TTL is still
// redeemable.
func TestTicketMintedAfterQueueWait(t *testing.T) {
	g := &gated{gate: make(chan struct{}), entered: make(chan string, 4)}
	pull := gatedPull{g}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, TicketTTL: time.Second})

	if _, err := p.Submit(context.Background(), g, aei.RunSpec{ID: "blocker", Deadline: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	<-g.entered
	if _, err := p.Submit(context.Background(), pull, aei.RunSpec{ID: "queued", Deadline: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // queued past the ticket TTL
	close(g.gate)
	waitTerminal(t, p, "queued")

	g.mu.Lock()
	cred := g.creds["queued"]
	g.mu.Unlock()
	if cred.Kind != aei.Ticket {
		t.Fatalf("credential kind = %v, want a ticket", cred.Kind)
	}
	if _, _, ok := p.redeem(cred.Value); !ok {
		t.Fatal("ticket minted for a queued run had already expired when its create started")
	}
}

// The provider is handed the run's remaining budget, not its full Deadline, so a
// workload cannot outlive the deadline the plane enforces.
func TestProviderGetsRemainingDeadline(t *testing.T) {
	g := &gated{gate: make(chan struct{}), entered: make(chan string, 4)}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1})

	if _, err := p.Submit(context.Background(), g, aei.RunSpec{ID: "blocker", Deadline: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	<-g.entered
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{ID: "queued", Deadline: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	close(g.gate)
	waitTerminal(t, p, "queued")

	g.mu.Lock()
	got := g.deadlines["queued"]
	g.mu.Unlock()
	if got <= 0 || got > 1600*time.Millisecond {
		t.Fatalf("provider got Deadline %v after 500ms queued on a 2s run, want <= 1.5s", got)
	}
}

// lateAccept is an AsyncLauncher whose Dispatch accepts the workload exactly as
// its context ends, returning a valid handle and no error.
type lateAccept struct{}

func (lateAccept) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (lateAccept) Dispatch(ctx context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	<-ctx.Done()
	return aei.Handle{Ref: spec.ID}, nil
}
func (lateAccept) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{ResultRef: "watched"}, nil
}

// A Dispatch that succeeds as the create timer fires is a dispatched run: the
// plane watches it rather than failing it (a retry would start a second workload).
func TestDispatchAcceptedAtCreateTimeoutIsWatched(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, CreateTimeout: 30 * time.Millisecond})
	id, err := p.Submit(context.Background(), lateAccept{}, aei.RunSpec{Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded || st.ResultRef != "watched" {
		t.Fatalf("run = %+v, want Succeeded via the watch", st)
	}
}

// slowMint is a gated provider whose credential step takes delay.
type slowMint struct {
	*gated
	delay time.Duration
}

func (s slowMint) PullsCredential() bool { time.Sleep(s.delay); return true }

// The provider's budget is measured after minting, just before Dispatch.
func TestRemainingBudgetMeasuredAfterMint(t *testing.T) {
	g := &gated{}
	p := New(Config{Secret: []byte("k"), Logger: quietLog})
	id, err := p.Submit(context.Background(), slowMint{g, 150 * time.Millisecond}, aei.RunSpec{ID: "slow-mint", Deadline: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, id)
	g.mu.Lock()
	got := g.deadlines["slow-mint"]
	g.mu.Unlock()
	if got <= 0 || got > 160*time.Millisecond {
		t.Fatalf("Dispatch got Deadline %v after a 150ms mint on a 300ms run, want <= 150ms", got)
	}
}

// countingPull's credential step blocks until release, counting calls in flight.
type countingPull struct {
	fakeWatch
	release        chan struct{}
	inflight, peak atomic.Int32
}

func (c *countingPull) PullsCredential() bool {
	n := c.inflight.Add(1)
	defer c.inflight.Add(-1)
	for {
		old := c.peak.Load()
		if n <= old || c.peak.CompareAndSwap(old, n) {
			break
		}
	}
	<-c.release
	return true
}

// Credential calls abandoned at the create timeout stay bounded: repeated runs
// against a hung credential step leave at most 2*MaxConcurrentCreates calls
// outstanding, and minting recovers once the backend answers.
func TestOutstandingMintsBounded(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 30 * time.Millisecond})
	hung := &countingPull{release: make(chan struct{})}
	for range 5 {
		id, err := p.Submit(context.Background(), hung, aei.RunSpec{})
		if err != nil {
			t.Fatal(err)
		}
		if st := mustTerminal(t, p, id); st.Phase != phaseFailed {
			t.Fatalf("run against a hung credential step = %+v, want Failed", st)
		}
	}
	if peak := hung.peak.Load(); peak != 2 {
		t.Fatalf("peak outstanding credential calls = %d, want 2 (2*MaxConcurrentCreates)", peak)
	}
	close(hung.release)
	waitFor(t, func() bool { return len(p.mints) == 0 })
	good, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded {
		t.Fatalf("run after the backend recovered = %+v, want Succeeded", st)
	}
}

// ctxLaunch is a plain Launcher that runs until its context ends, recording the
// Deadline it was handed.
type ctxLaunch struct{ handed chan time.Duration }

func (c ctxLaunch) Launch(ctx context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	c.handed <- spec.Deadline
	<-ctx.Done()
	return aei.Handle{}, ctx.Err()
}

// A plain provider's Launch holds its create slot until it returns, so a run with
// no deadline still gets one; one that waits on ctx then ends and frees the slot.
func TestPlainLaunchGetsFallbackDeadline(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1})
	p.launchDeadline = 100 * time.Millisecond
	l := ctxLaunch{handed: make(chan time.Duration, 1)}
	id, err := p.Submit(context.Background(), l, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if d := <-l.handed; d <= 0 || d > 100*time.Millisecond {
		t.Errorf("Launch was handed Deadline %v, want the fallback (<= 100ms)", d)
	}
	if st := mustTerminal(t, p, id); st.Phase != phaseFailed {
		t.Fatalf("deadline-less plain run = %+v, want Failed at the fallback deadline", st)
	}
	good, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded {
		t.Fatalf("healthy run after = %+v, want Succeeded", st)
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
