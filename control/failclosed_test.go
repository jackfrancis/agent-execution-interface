package control

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

func mustToken(t *testing.T, p *Plane, c Claims) string {
	t.Helper()
	tok, err := p.mintToken(c)
	if err != nil {
		t.Fatalf("mintToken: %v", err)
	}
	return tok
}

func mustMint(t *testing.T, p *Plane, l aei.Launcher, spec aei.RunSpec) aei.Credential {
	t.Helper()
	cred, err := p.mint(l, spec)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	return cred
}

// failingAuthority cannot mint (e.g. a broken or unreachable signing key).
type failingAuthority struct{ empty bool }

func (a failingAuthority) Mint(Claims) (string, error) {
	if a.empty {
		return "", nil // a buggy authority: no error, but no token either
	}
	return "", errors.New("signing key unavailable")
}
func (failingAuthority) Verify(string) (Claims, bool) { return Claims{}, false }

// countingLauncher records whether the plane dispatched at all.
type countingLauncher struct{ launched int }

func (l *countingLauncher) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	l.launched++
	return aei.Handle{}, nil
}

// legacyEmptyKeyToken was minted by the pre-fail-closed HMAC authority with an
// EMPTY secret (exp 2100-01-01): what an unkeyed plane used to hand out. Anyone
// can forge such a token, so an unkeyed authority must never accept it.
const legacyEmptyKeyToken = "eyJydW4iOiJsZWdhY3kiLCJzdWIiOiJ1MSIsImV4cCI6NDEwMjQ0NDgwMH0.h-Tsiz8LIY0EAeJpDnEgLloq7Er4iaw0hhu8u6D62ls"

// An unkeyed HMAC authority must not mint (an empty key is forgeable) nor verify.
func TestHMACAuthorityWithoutSecretFailsClosed(t *testing.T) {
	a := hmacAuthority{now: time.Now}
	if tok, err := a.Mint(Claims{RunID: "r", Exp: time.Now().Add(time.Hour).Unix()}); err == nil {
		t.Fatalf("unkeyed HMAC minted %q; want an error", tok)
	}
	// The fixture really is a valid empty-key signature, so only the empty-key
	// guard in Verify stands between it and acceptance.
	b, sig, _ := strings.Cut(legacyEmptyKeyToken, ".")
	if a.sign(b) != sig {
		t.Fatal("fixture is not an empty-key HMAC signature")
	}
	if c, ok := a.Verify(legacyEmptyKeyToken); ok {
		t.Fatalf("unkeyed HMAC verified a forgeable empty-key token: %+v", c)
	}
	if _, ok := New(Config{}).verifyToken(legacyEmptyKeyToken); ok {
		t.Fatal("an unkeyed plane verified a forgeable empty-key token")
	}
}

// Run (the AgentRun path): a mint error fails the run and never dispatches it.
func TestRunFailsWhenMintFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"authority error", Config{Authority: failingAuthority{}}},
		{"empty token", Config{Authority: failingAuthority{empty: true}}},
		{"no secret", Config{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(tc.cfg)
			l := &countingLauncher{}
			res, err := p.Run(context.Background(), l, aei.RunSpec{ID: "run-m"})
			if err == nil || res.Err == nil {
				t.Fatalf("Run = (%+v, %v); want a mint error", res, err)
			}
			if !strings.Contains(res.Err.Error(), "mint run credential") {
				t.Errorf("error %q should name the mint failure", res.Err)
			}
			if l.launched != 0 {
				t.Errorf("launched %d times; a run without a credential must not dispatch", l.launched)
			}
		})
	}
}

// recordingObserver captures each run observation (Submit observes from its own
// goroutine, hence the lock).
type recordingObserver struct {
	mu   sync.Mutex
	runs []RunObservation
}

func (o *recordingObserver) ObserveRun(r RunObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.runs = append(o.runs, r)
}

func (o *recordingObserver) snapshot() []RunObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]RunObservation(nil), o.runs...)
}

// waitFailed polls a submitted run until it is Failed (minting runs under the
// run's deadline, after Submit returns) and returns its state.
func waitFailed(t *testing.T, p *Plane, id string) RunState {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if st, _ := p.Status(id); st.Phase == phaseFailed {
			return st
		}
		if time.Now().After(deadline) {
			st, _ := p.Status(id)
			t.Fatalf("run %s never failed: %+v", id, st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Submit (the data-plane path): a mint error records the run Failed, visible at
// GET /runs/{id}, and never dispatches it.
func TestSubmitMarksRunFailedWhenMintFails(t *testing.T) {
	obs := &recordingObserver{}
	p := New(Config{Authority: failingAuthority{}, Observer: obs})
	l := &countingLauncher{}
	id, err := p.Submit(context.Background(), l, aei.RunSpec{ID: "run-s", TaskRef: "t"}, WithProvider("k8s-job"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st := waitFailed(t, p, id)
	if !strings.Contains(st.Err, "signing key unavailable") {
		t.Errorf("status error %q should carry the mint failure", st.Err)
	}
	if l.launched != 0 {
		t.Errorf("launched %d times; a run without a credential must not dispatch", l.launched)
	}
	// The observation follows the store write in the same goroutine.
	var runs []RunObservation
	for end := time.Now().Add(time.Second); len(runs) == 0 && time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		runs = obs.snapshot()
	}
	if len(runs) != 1 {
		t.Fatalf("observed %d runs; want exactly 1", len(runs))
	}
	if r := runs[0]; r.Phase != phaseFailed || r.Provider != "k8s-job" || r.TaskRef != "t" {
		t.Errorf("observation = %+v; want Failed, attributed to k8s-job and task t", r)
	}
}

// Redeem (the pull path): a mint error at redemption returns 500 — never a 200
// with an empty credential — and fails the run.
func TestRedeemFailsRunWhenMintFails(t *testing.T) {
	p := New(Config{Authority: failingAuthority{}})
	// A ticket is issued without minting, so the mint error surfaces at redeem.
	cred := mustMint(t, p, &fakePull{}, aei.RunSpec{ID: "run-p"})
	if cred.Kind != aei.Ticket {
		t.Fatalf("want a ticket, got %v", cred.Kind)
	}
	p.store.SetRunning("run-p")
	done := p.arm("run-p")
	defer p.disarm("run-p")

	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	resp := post(t, srv.URL+"/aei/v1alpha1/redeem", "", map[string]string{"ticket": cred.Value})
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("redeem status = %d; want 500", resp.StatusCode)
	}
	if st, _ := p.Status("run-p"); st.Phase != phaseFailed {
		t.Errorf("run phase = %q; want Failed", st.Phase)
	}
	select {
	case r := <-done:
		if r.Err == nil {
			t.Error("waiter woke with no error")
		}
	default:
		t.Error("a waiting Run/Submit was not woken with the failure")
	}
}

// redeemingDispatcher is a pull provider whose runtime redeems its ticket while
// Dispatch is still running (as agent-substrate does) and reports only the HTTP
// symptom when redemption fails.
type redeemingDispatcher struct {
	fakeWatch
	p *Plane // the replica the runtime's /redeem lands on
}

func (redeemingDispatcher) PullsCredential() bool { return true }
func (d redeemingDispatcher) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	if _, _, ok, err := d.p.redeem(cred.Value); !ok || err != nil {
		return aei.Handle{}, errors.New("redeem status 500")
	}
	return aei.Handle{Ref: spec.ID}, nil
}

// A mint error at redemption during Dispatch reaches the AgentRun (Run's result)
// and the run store as the cause, not as the provider's "redeem status 500".
func TestRedeemDuringDispatchKeepsSigningError(t *testing.T) {
	p := New(Config{Authority: failingAuthority{}})
	res, err := p.Run(context.Background(), &redeemingDispatcher{p: p}, aei.RunSpec{ID: "run-sub"})
	if res.Err == nil || !strings.Contains(res.Err.Error(), "signing key unavailable") {
		t.Fatalf("Run result err = %v; want the signing error", res.Err)
	}
	// The runtime's recorded outcome and the provider's report race; either way
	// the error Run returns is nil or that same cause, never "redeem status 500".
	if err != nil && !strings.Contains(err.Error(), "signing key unavailable") {
		t.Fatalf("Run err = %v; want nil or the signing error", err)
	}

	p2 := New(Config{Authority: failingAuthority{}})
	id, _ := p2.Submit(context.Background(), &redeemingDispatcher{p: p2}, aei.RunSpec{ID: "run-sub2"})
	if st := waitFailed(t, p2, id); !strings.Contains(st.Err, "signing key unavailable") {
		t.Fatalf("status err = %q; want the signing error", st.Err)
	}
}

// With replicas > 1 the runtime's /redeem can land on another replica, which
// records the failure in the shared Store but cannot wake the dispatching
// replica's waiter. The dispatcher still reports the signing error, not the
// provider's "redeem status 500".
func TestRedeemOnAnotherReplicaKeepsSigningError(t *testing.T) {
	shared := newMemStore()
	dispatcher := New(Config{Authority: failingAuthority{}, Store: shared})
	receiver := New(Config{Authority: failingAuthority{}, Store: shared})

	res, err := dispatcher.Run(context.Background(), &redeemingDispatcher{p: receiver}, aei.RunSpec{ID: "run-ha-sign"})
	if res.Err == nil || !strings.Contains(res.Err.Error(), "signing key unavailable") {
		t.Fatalf("Run result err = %v; want the signing error", res.Err)
	}
	// The runtime's recorded outcome and the provider's report race; either way
	// the error Run returns is nil or that same cause, never "redeem status 500".
	if err != nil && !strings.Contains(err.Error(), "signing key unavailable") {
		t.Fatalf("Run err = %v; want nil or the signing error", err)
	}

	id, _ := dispatcher.Submit(context.Background(), &redeemingDispatcher{p: receiver}, aei.RunSpec{ID: "run-ha-sign2"})
	if st := waitFailed(t, dispatcher, id); !strings.Contains(st.Err, "signing key unavailable") {
		t.Fatalf("status err = %q; want the signing error", st.Err)
	}
}

// stallingStore is a shared Store whose Run reads hang until release closes (a
// slow or partitioned backing store).
type stallingStore struct {
	*memStore
	release chan struct{}
}

func (s stallingStore) Run(runID string) (RunState, bool) {
	<-s.release
	return s.memStore.Run(runID)
}

// The Store lookup that follows a mint error honors the run's deadline: a stalled
// shared Store cannot hold the run open past it.
func TestStalledStoreLookupHonorsDeadline(t *testing.T) {
	st := stallingStore{memStore: newMemStore(), release: make(chan struct{})}
	defer close(st.release)
	p := New(Config{Authority: failingAuthority{}, Store: st})

	const deadline = 100 * time.Millisecond
	type out struct {
		res aei.Result
		err error
	}
	got := make(chan out, 1)
	start := time.Now()
	go func() {
		res, err := p.Run(context.Background(), &countingLauncher{}, aei.RunSpec{ID: "run-stall", Deadline: deadline})
		got <- out{res, err}
	}()
	select {
	case o := <-got:
		if elapsed := time.Since(start); elapsed > deadline+300*time.Millisecond {
			t.Errorf("Run returned after %v; want about its %v deadline", elapsed, deadline)
		}
		if o.res.Err == nil || !strings.Contains(o.res.Err.Error(), "signing key unavailable") {
			t.Errorf("Run result err = %v; want the mint error", o.res.Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run blocked on a stalled Store read past its deadline")
	}
}

// lateAckDispatcher is a pull provider whose runtime redeems its ticket on
// another replica shortly after dispatch, while the provider's own Dispatch
// acknowledges only after the run's deadline.
type lateAckDispatcher struct {
	fakeWatch
	receiver    *Plane
	recordAfter time.Duration // when the peer's /redeem records the failure (default 30ms)
	ackAfter    time.Duration
}

func (lateAckDispatcher) PullsCredential() bool { return true }
func (d lateAckDispatcher) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	go func() {
		time.Sleep(cmp.Or(d.recordAfter, 30*time.Millisecond))
		d.receiver.redeem(cred.Value) // fails to mint; records the failure in the shared Store
	}()
	time.Sleep(d.ackAfter) // ignores cancellation, like a slow provider
	return aei.Handle{}, errors.New("redeem status 500")
}

// A signing failure another replica records before the deadline is the run's
// outcome, even when the provider's acknowledgment arrives after the deadline:
// Run ends with it, within the deadline, and agrees with Status.
func TestFailureRecordedBeforeTimeoutWins(t *testing.T) {
	shared := newMemStore()
	dispatcher := New(Config{Authority: failingAuthority{}, Store: shared})
	receiver := New(Config{Authority: failingAuthority{}, Store: shared})

	const deadline = 300 * time.Millisecond
	start := time.Now()
	res, _ := dispatcher.Run(context.Background(),
		&lateAckDispatcher{receiver: receiver, ackAfter: 3 * deadline},
		aei.RunSpec{ID: "run-late-ack", Deadline: deadline})
	elapsed := time.Since(start)

	if res.Err == nil || !strings.Contains(res.Err.Error(), "signing key unavailable") {
		t.Fatalf("Run result err = %v; want the signing failure recorded before the deadline", res.Err)
	}
	if errors.Is(res.Err, ErrDeadlineExceeded) {
		t.Fatalf("Run reported the deadline, not the recorded cause: %v", res.Err)
	}
	if elapsed >= deadline {
		t.Errorf("Run took %v; want it to end within its %v deadline", elapsed, deadline)
	}
	if st, _ := dispatcher.Status("run-late-ack"); !strings.Contains(st.Err, "signing key unavailable") {
		t.Errorf("Status err = %q; Run and Status disagree", st.Err)
	}
}

// hungStore is a shared Store whose Run and FinishRun calls hang until release
// closes, counting how many are outstanding at once.
type hungStore struct {
	*memStore
	release           chan struct{}
	mu                sync.Mutex
	inflight, maxSeen int
}

func (s *hungStore) enter() {
	s.mu.Lock()
	s.inflight++
	s.maxSeen = max(s.maxSeen, s.inflight)
	s.mu.Unlock()
	<-s.release
	s.mu.Lock()
	s.inflight--
	s.mu.Unlock()
}
func (s *hungStore) Run(id string) (RunState, bool) { s.enter(); return s.memStore.Run(id) }
func (s *hungStore) FinishRun(id string, st RunState) bool {
	s.enter()
	return s.memStore.FinishRun(id, st)
}
func (s *hungStore) peak() int { s.mu.Lock(); defer s.mu.Unlock(); return s.maxSeen }

// blockUntilDone is a plain provider whose Launch runs until the run's context ends.
type blockUntilDone struct{}

func (blockUntilDone) Launch(ctx context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	<-ctx.Done()
	return aei.Handle{Ref: spec.ID}, ctx.Err()
}

// During a Store outage, the run watch and Run's commit cannot pile up stuck
// Store calls: however many runs go by, outstanding calls stay within the plane's
// Store permits, and each Run still returns shortly after its deadline.
func TestStoreOutageBoundsOutstandingCalls(t *testing.T) {
	st := &hungStore{memStore: newMemStore(), release: make(chan struct{})}
	defer close(st.release)
	p := New(Config{Secret: []byte("k"), Store: st, Logger: quietLog, MaxConcurrentCreates: 1})
	const limit = 2 // the Store permits: 2*MaxConcurrentCreates

	const deadline = 60 * time.Millisecond
	for i := range 8 {
		start := time.Now()
		res, _ := p.Run(context.Background(), blockUntilDone{}, aei.RunSpec{ID: fmt.Sprintf("outage-%d", i), Deadline: deadline})
		if elapsed := time.Since(start); elapsed > deadline+minCommitGrace+300*time.Millisecond {
			t.Fatalf("run %d took %v during the outage; want about its deadline plus the commit grace", i, elapsed)
		}
		if res.Err == nil {
			t.Fatalf("run %d succeeded; want the deadline", i)
		}
	}
	if n := st.peak(); n > limit {
		t.Fatalf("%d Store calls outstanding at once; want at most the %d Store permits", n, limit)
	}
}

// A failure a peer replica records after the watch's last poll — 25ms before the
// deadline — is still the run's outcome: Run commits through the Store, finds the
// peer's outcome first, and agrees with Status.
func TestFailureRecordedJustBeforeDeadlineWins(t *testing.T) {
	shared := newMemStore()
	dispatcher := New(Config{Authority: failingAuthority{}, Store: shared, Logger: quietLog})
	receiver := New(Config{Authority: failingAuthority{}, Store: shared, Logger: quietLog})

	const deadline = 300 * time.Millisecond
	res, _ := dispatcher.Run(context.Background(),
		&lateAckDispatcher{receiver: receiver, recordAfter: deadline - 25*time.Millisecond, ackAfter: 3 * deadline},
		aei.RunSpec{ID: "run-last-moment", Deadline: deadline})

	if res.Err == nil || !strings.Contains(res.Err.Error(), "signing key unavailable") {
		t.Fatalf("Run result err = %v; want the signing failure recorded before the deadline", res.Err)
	}
	if st, _ := dispatcher.Status("run-last-moment"); st.Err != res.Err.Error() {
		t.Errorf("Status err = %q, Run err = %q; they disagree", st.Err, res.Err)
	}
}

// completingLauncher is a plain provider whose runtime's /complete lands on a
// peer replica (with a result and timing) before Launch itself returns success.
type completingLauncher struct {
	t       *testing.T
	peerURL string
}

func (l completingLauncher) Launch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	resp := post(l.t, l.peerURL+"/aei/v1alpha1/complete", cred.Value, map[string]any{
		"run_id": spec.ID, "result_ref": "res://peer", "timing": map[string]any{"tool_calls": 2},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		l.t.Errorf("peer /complete status = %d, want 202", resp.StatusCode)
	}
	return aei.Handle{Ref: spec.ID}, nil // success, carrying no result of its own
}

// A completion a peer accepted before the provider returned success is the run's
// outcome: Run reports the peer's result reference and timing, as Status does,
// not the provider's empty success.
func TestPeerCompletionWinsOverProviderSuccess(t *testing.T) {
	shared := newMemStore()
	secret := []byte("shared-secret")
	dispatcher := New(Config{Secret: secret, Store: shared, Logger: quietLog})
	receiver := New(Config{Secret: secret, Store: shared, Logger: quietLog})
	srv := httptest.NewServer(receiver.Handler())
	defer srv.Close()

	res, err := dispatcher.Run(context.Background(), completingLauncher{t: t, peerURL: srv.URL}, aei.RunSpec{ID: "run-peer-done", Deadline: 2 * time.Second})
	if err != nil || res.Err != nil {
		t.Fatalf("Run = (%+v, %v); want success", res, err)
	}
	if res.ResultRef != "res://peer" || res.Timing.ToolCalls != 2 {
		t.Fatalf("Run result = ref %q, tool calls %d; want the peer's res://peer and 2", res.ResultRef, res.Timing.ToolCalls)
	}
	if st, _ := dispatcher.Status("run-peer-done"); st.ResultRef != res.ResultRef || st.Timing.ToolCalls != res.Timing.ToolCalls {
		t.Errorf("Status = %+v; Run and Status disagree", st)
	}
}
