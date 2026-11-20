package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
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
	deadlines map[string]time.Duration // budget left (to the run's cutoff) at each Dispatch
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
	// The budget the provider has left: until the cutoff the plane enforces.
	g.deadlines[spec.ID], g.creds[spec.ID] = time.Until(spec.Cutoff(time.Now())), cred
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
		{"mint", &panicPull{}, "PullsCredential"},
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
	if got := obs.snapshot(); len(got) != 1 || got[0].Phase != phaseFailed {
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
	if st := mustTerminal(t, p, id); st.Phase != phaseFailed || !strings.Contains(st.Err, "PullsCredential not ready") {
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
	waitFor(t, func() bool { return capabilityPools(p) == 0 })
	good, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded {
		t.Fatalf("run after the backend recovered = %+v, want Succeeded", st)
	}
}

// ctxLaunch is a plain Launcher that runs until its context ends, recording the
// budget it was handed (until the run's cutoff).
type ctxLaunch struct{ handed chan time.Duration }

func (c ctxLaunch) Launch(ctx context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	c.handed <- time.Until(spec.Cutoff(time.Now()))
	<-ctx.Done()
	return aei.Handle{}, ctx.Err()
}

// A plain provider's Launch holds its create slot until it returns, so a run
// dispatched with no timeout must still have a deadline: the plane default. A
// Launch that waits on ctx then ends and frees the slot.
func TestPlainLaunchWithoutTimeoutEndsAtDefaultDeadline(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, DefaultDeadline: 100 * time.Millisecond})
	l := ctxLaunch{handed: make(chan time.Duration, 1)}
	id, err := p.Submit(context.Background(), l, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if d := <-l.handed; d <= 0 || d > 100*time.Millisecond {
		t.Errorf("Launch was handed a budget of %v, want the default deadline (<= 100ms)", d)
	}
	if st := mustTerminal(t, p, id); st.Phase != phaseFailed {
		t.Fatalf("plain run without a timeout = %+v, want Failed at the default deadline", st)
	}
	good, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded {
		t.Fatalf("healthy run after = %+v, want Succeeded", st)
	}
}

// stallStore is a Store whose ticket writes block until release, standing in
// for a ticket-store outage; everything else is the in-memory store.
type stallStore struct {
	*memStore
	release chan struct{}
	writes  atomic.Int32
}

func (s *stallStore) PutTicket(id string, c Claims, expires time.Time) {
	s.writes.Add(1)
	<-s.release
	s.memStore.PutTicket(id, c, expires)
}

// A ticket-store outage does not stop runs whose credential the authority signs
// locally: with every ticket permit held by stalled writes, a pushed-token run
// (a Kubernetes Job) still mints and dispatches, while more pull runs are bounded.
func TestTicketStoreOutageDoesNotBlockTokenRuns(t *testing.T) {
	store := &stallStore{memStore: newMemStore(), release: make(chan struct{})}
	p := New(Config{Secret: []byte("k"), Logger: quietLog, Store: store, MaxConcurrentCreates: 1, CreateTimeout: 30 * time.Millisecond})

	for range 3 { // two stall in the store and keep both ticket permits; the third waits for one
		id, err := p.Submit(context.Background(), &fakePull{fakeWatch{result: aei.Result{ResultRef: "pulled"}}}, aei.RunSpec{})
		if err != nil {
			t.Fatal(err)
		}
		if st := mustTerminal(t, p, id); st.Phase != phaseFailed || !strings.Contains(st.Err, "redemption ticket not ready") {
			t.Fatalf("pull run during the outage = %+v, want Failed waiting on the ticket store", st)
		}
	}
	if w := store.writes.Load(); w != 2 {
		t.Fatalf("ticket writes outstanding = %d, want 2 (2*MaxConcurrentCreates)", w)
	}

	g := &gated{}
	job, err := p.Submit(context.Background(), g, aei.RunSpec{ID: "job", Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, job); st.Phase != phaseSucceeded {
		t.Fatalf("token run during the ticket-store outage = %+v, want Succeeded", st)
	}
	g.mu.Lock()
	cred := g.creds["job"]
	g.mu.Unlock()
	if cred.Kind != aei.Token || cred.Value == "" {
		t.Fatalf("job credential = %+v, want a signed token", cred)
	}
	close(store.release) // only now does the store recover
}

func capabilityPools(p *Plane) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.providerPools)
}

// A credential's expiry is set once its step holds a permit, not when the run
// starts waiting for one: a run that waited longer than the token lifetime for
// capacity still gets a credential that verifies (a token, or a ticket that
// redeems for one).
func TestCredentialExpirySetAfterPermitWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		pull bool
	}{{"token", false}, {"ticket", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 5 * time.Second, TokenTTL: time.Second, TicketTTL: 10 * time.Second})
			permits := p.tokenCalls
			g := &gated{}
			var l aei.Launcher = g
			if tc.pull {
				permits, l = p.ticketCalls, gatedPull{g}
			}
			for range cap(permits) { // every permit held by stalled calls
				permits <- struct{}{}
			}
			id, err := p.Submit(context.Background(), l, aei.RunSpec{ID: "job", Deadline: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			// Wait longer than the token lifetime; expiry has whole-second
			// resolution, so more than 2s guarantees crossing it.
			time.Sleep(2200 * time.Millisecond)
			for range cap(permits) {
				<-permits
			}
			if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded {
				t.Fatalf("run = %+v, want Succeeded", st)
			}
			g.mu.Lock()
			cred := g.creds["job"]
			g.mu.Unlock()
			tok := cred.Value
			if tc.pull {
				var ok bool
				if tok, _, ok = p.redeem(cred.Value); !ok {
					t.Fatal("ticket did not redeem")
				}
			}
			if _, ok := p.verifyToken(tok); !ok {
				t.Fatal("credential minted after waiting for a permit had already expired")
			}
		})
	}
}

// PullsCredential permits are per launcher: one provider stuck there does not stop
// another provider's runs, while the stuck provider's own calls stay bounded.
func TestCapabilityChecksIsolatedPerLauncher(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 30 * time.Millisecond})
	stuck := &countingPull{release: make(chan struct{})}
	for i := range 3 {
		id, err := p.Submit(context.Background(), stuck, aei.RunSpec{})
		if err != nil {
			t.Fatal(err)
		}
		st := mustTerminal(t, p, id)
		if st.Phase != phaseFailed {
			t.Fatalf("stuck provider run %d = %+v, want Failed", i, st)
		}
		if i == 2 && !strings.Contains(st.Err, "earlier calls still outstanding") {
			t.Fatalf("third stuck run = %+v, want refused a permit (its own calls are bounded)", st)
		}
	}

	// Another provider that also implements PullsCredential, while the first
	// provider's calls are still blocked.
	healthy := &fakePull{fakeWatch{result: aei.Result{ResultRef: "fine"}}}
	good, err := p.Submit(context.Background(), healthy, aei.RunSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, good); st.Phase != phaseSucceeded {
		t.Fatalf("healthy provider while another is stuck in PullsCredential = %+v, want Succeeded", st)
	}
	if peak := stuck.peak.Load(); peak != 2 {
		t.Errorf("stuck provider's outstanding PullsCredential calls = %d, want 2", peak)
	}

	close(stuck.release)
	waitFor(t, func() bool { return capabilityPools(p) == 0 }) // no entry outlives its calls
}

// valuePull is a launcher used by value whose type is comparable but whose value
// is not: cfg, an interface field, holds a slice. Its PullsCredential blocks on
// block when that is set.
type valuePull struct {
	cfg   any
	block chan struct{}
}

func (v valuePull) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (v valuePull) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	return aei.Handle{Ref: spec.ID}, nil
}
func (v valuePull) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{ResultRef: "value"}, nil
}
func (v valuePull) PullsCredential() bool {
	if v.block != nil {
		<-v.block
	}
	return true
}

// A launcher whose type is comparable but whose value is not (an interface field
// holding a slice) must not panic as a map key while the plane's lock is held:
// that left the mutex locked and every later submission and callback blocked.
func TestNonComparableLauncherValueDoesNotWedgePlane(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog})
	l := valuePull{cfg: []string{"a", "b"}}
	for i := range 2 {
		id, err := p.Submit(context.Background(), l, aei.RunSpec{})
		if err != nil {
			t.Fatal(err)
		}
		if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded {
			t.Fatalf("run %d with a non-comparable launcher value = %+v, want Succeeded", i, st)
		}
	}
	if r, err := p.Run(context.Background(), l, aei.RunSpec{Deadline: 2 * time.Second}); err != nil || r.Err != nil {
		t.Fatalf("Run with a non-comparable launcher value = %+v, %v", r, err)
	}
	locked := make(chan struct{})
	go func() { p.mu.Lock(); p.mu.Unlock(); close(locked) }()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("the plane's mutex was left locked")
	}
}

// Separately configured launchers of one type, used by value with no comparable
// identity, get their own PullsCredential permits when the caller names their
// provider class, on both Submit and Run.
func TestValueLaunchersIsolatedByProviderClass(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 30 * time.Millisecond})
	stuck := valuePull{cfg: []string{"stuck"}, block: make(chan struct{})}
	defer close(stuck.block)
	for range 2 { // both of the stuck class's permits held by blocked calls
		id, err := p.Submit(context.Background(), stuck, aei.RunSpec{}, WithProviderClass("stuck"))
		if err != nil {
			t.Fatal(err)
		}
		if st := mustTerminal(t, p, id); st.Phase != phaseFailed {
			t.Fatalf("stuck class run = %+v, want Failed", st)
		}
	}

	healthy := valuePull{cfg: []string{"healthy"}}
	id, err := p.Submit(context.Background(), healthy, aei.RunSpec{}, WithProviderClass("healthy"))
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded {
		t.Fatalf("healthy class (Submit) while another is stuck = %+v, want Succeeded", st)
	}
	r, err := p.RunWithOptions(context.Background(), healthy, aei.RunSpec{Deadline: 2 * time.Second}, WithProviderClass("healthy"))
	if err != nil || r.Err != nil || r.ResultRef != "value" {
		t.Fatalf("healthy class (Run) while another is stuck = %+v, %v; want Succeeded", r, err)
	}
}

// Run keeps the signature callers may have declared in their own interfaces.
var _ interface {
	Run(context.Context, aei.Launcher, aei.RunSpec) (aei.Result, error)
} = (*Plane)(nil)

// deafDispatch is an AsyncLauncher whose Dispatch ignores its context: it returns
// a handle only when release is closed, however long that takes.
type deafDispatch struct {
	release chan struct{}
	calls   *atomic.Int32
}

func (d deafDispatch) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (d deafDispatch) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	d.calls.Add(1)
	defer d.calls.Add(-1)
	<-d.release
	return aei.Handle{Ref: spec.ID}, nil
}
func (d deafDispatch) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{ResultRef: "late"}, nil
}

// A Dispatch that ignores cancellation holds the global create slot only until
// the create timeout. While it stays blocked, healthy providers dispatch; the deaf
// provider's own in-flight calls are bounded (2*MaxConcurrentCreates), its next
// run is refused at once, and a workload it accepts late is watched, not orphaned.
func TestDeafDispatchDoesNotHoldGlobalSlot(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 30 * time.Millisecond})
	deaf := deafDispatch{release: make(chan struct{}), calls: new(atomic.Int32)}

	var deafRuns []string
	for range 2 { // fill the deaf provider's in-flight permits
		id, err := p.Submit(context.Background(), deaf, aei.RunSpec{Deadline: 10 * time.Second}, WithProviderClass("deaf"))
		if err != nil {
			t.Fatal(err)
		}
		deafRuns = append(deafRuns, id)
	}
	waitFor(t, func() bool { return deaf.calls.Load() == 2 })

	refused, err := p.Submit(context.Background(), deaf, aei.RunSpec{Deadline: 10 * time.Second}, WithProviderClass("deaf"))
	if err != nil {
		t.Fatal(err)
	}
	if st := mustTerminal(t, p, refused); st.Phase != phaseFailed || !strings.Contains(st.Err, "still in flight past the create timeout") {
		t.Fatalf("third deaf run = %+v, want refused at once", st)
	}

	// Healthy dispatch while both deaf calls are still blocked.
	for _, path := range []string{"submit", "run"} {
		if path == "submit" {
			id, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{Deadline: 2 * time.Second}, WithProviderClass("healthy"))
			if err != nil {
				t.Fatal(err)
			}
			if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded {
				t.Fatalf("healthy Submit while a Dispatch ignores cancellation = %+v, want Succeeded", st)
			}
		} else if r, err := p.RunWithOptions(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "fine"}}, aei.RunSpec{Deadline: 2 * time.Second}, WithProviderClass("healthy")); err != nil || r.Err != nil {
			t.Fatalf("healthy Run while a Dispatch ignores cancellation = %+v, %v", r, err)
		}
	}
	if n := deaf.calls.Load(); n != 2 {
		t.Fatalf("deaf Dispatch calls in flight = %d, want still 2", n)
	}

	close(deaf.release) // the deaf provider accepts both workloads, late
	for _, id := range deafRuns {
		if st := mustTerminal(t, p, id); st.Phase != phaseSucceeded || st.ResultRef != "late" {
			t.Fatalf("late-accepted deaf run = %+v, want watched to Succeeded", st)
		}
	}
	waitFor(t, func() bool { return capabilityPools(p) == 0 })
}

// A plain run that never reached Launch (it waited in the queue past its
// deadline) is reported as undispatched.
func TestQueuedPlainRunReportedUndispatched(t *testing.T) {
	g := &gated{gate: make(chan struct{}), entered: make(chan string, 1)}
	defer close(g.gate)
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1})
	if _, err := p.Submit(context.Background(), g, aei.RunSpec{Deadline: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	<-g.entered // a healthy create holds the only slot

	l := ctxLaunch{handed: make(chan time.Duration, 1)}
	r, err := p.Run(context.Background(), l, aei.RunSpec{Deadline: 50 * time.Millisecond})
	if err == nil || r.Err == nil || !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("queued plain Run = %+v, %v; want a dispatch error at the deadline", r, err)
	}
	select {
	case <-l.handed:
		t.Fatal("Launch was entered")
	default:
	}
}

// nanPull is a value launcher whose only field is NaN: comparable, but never
// equal to itself. Its PullsCredential blocks until release.
type nanPull struct {
	f       float64
	release chan struct{}
	calls   *atomic.Int32
}

func (n nanPull) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	return aei.Handle{}, nil
}
func (n nanPull) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	return aei.Handle{Ref: spec.ID}, nil
}
func (n nanPull) Await(context.Context, aei.Handle) (aei.Result, error) {
	return aei.Result{}, nil
}
func (n nanPull) PullsCredential() bool {
	n.calls.Add(1)
	defer n.calls.Add(-1)
	<-n.release
	return true
}

// A launcher value containing NaN still maps to one permit pool, so its calls stay
// bounded instead of each run creating a fresh pool.
func TestNaNLauncherStaysBounded(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Logger: quietLog, MaxConcurrentCreates: 1, CreateTimeout: 30 * time.Millisecond})
	l := nanPull{f: math.NaN(), release: make(chan struct{}), calls: new(atomic.Int32)}
	defer close(l.release)
	for i := range 4 {
		id, err := p.Submit(context.Background(), l, aei.RunSpec{})
		if err != nil {
			t.Fatal(err)
		}
		if st := mustTerminal(t, p, id); st.Phase != phaseFailed {
			t.Fatalf("run %d = %+v, want Failed", i, st)
		}
	}
	if n := l.calls.Load(); n != 2 {
		t.Fatalf("PullsCredential calls in flight = %d, want 2 (one bounded pool)", n)
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
