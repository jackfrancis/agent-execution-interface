package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// stubbornAsync is an AsyncLauncher whose Await ignores cancellation: it returns
// only when the test closes release, long after the run's deadline. It records the
// spec it was dispatched with, so a test can read the deadline the plane applied.
type stubbornAsync struct {
	specs   chan aei.RunSpec
	release chan struct{}
	late    aei.Result // what Await reports once released
}

func newStubbornAsync(t *testing.T) *stubbornAsync {
	s := &stubbornAsync{specs: make(chan aei.RunSpec, 1), release: make(chan struct{}), late: aei.Result{ResultRef: "late"}}
	t.Cleanup(func() { close(s.release) })
	return s
}

func (s *stubbornAsync) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, _ := s.Dispatch(ctx, spec, cred)
	r, _ := s.Await(ctx, h)
	return h, r.Err
}
func (s *stubbornAsync) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	s.specs <- spec
	return aei.Handle{Provider: "stubborn", Ref: spec.ID}, nil
}
func (s *stubbornAsync) Await(context.Context, aei.Handle) (aei.Result, error) {
	<-s.release // ignores ctx
	return s.late, nil
}

// stubbornLaunch is a plain (non-async) Launcher whose Launch ignores
// cancellation the same way.
type stubbornLaunch struct{ release chan struct{} }

func newStubbornLaunch(t *testing.T) *stubbornLaunch {
	s := &stubbornLaunch{release: make(chan struct{})}
	t.Cleanup(func() { close(s.release) })
	return s
}

func (s *stubbornLaunch) Launch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	<-s.release // ignores ctx
	return aei.Handle{Ref: spec.ID}, nil
}

// awaitRunState polls Status until the run leaves Running, failing the test after
// limit.
func awaitRunState(t *testing.T, p *Plane, runID string, limit time.Duration) RunState {
	t.Helper()
	end := time.Now().Add(limit)
	for time.Now().Before(end) {
		if st, ok := p.Status(runID); ok && st.Phase != phaseRunning {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s still running after %s", runID, limit)
	return RunState{}
}

func assertTimedOut(t *testing.T, st RunState) {
	t.Helper()
	if st.Phase != phaseFailed {
		t.Errorf("phase = %q, want %q", st.Phase, phaseFailed)
	}
	if !strings.Contains(st.Err, ErrDeadlineExceeded.Error()) {
		t.Errorf("err = %q, want it to contain %q", st.Err, ErrDeadlineExceeded.Error())
	}
}

// TestSubmitWithoutTimeoutEndsAtDefault: a dispatch with no timeout gets the
// plane's default deadline (also handed to the provider), and the plane ends the
// run there even though the provider never reports.
func TestSubmitWithoutTimeoutEndsAtDefault(t *testing.T) {
	const def = 50 * time.Millisecond
	fp := newStubbornAsync(t)
	p := New(Config{Secret: []byte("k"), DefaultDeadline: def})

	start := time.Now()
	id, err := p.Submit(context.Background(), fp, aei.RunSpec{ID: "no-timeout"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := (<-fp.specs).Deadline; got != def {
		t.Errorf("dispatched deadline = %s, want the default %s", got, def)
	}
	st := awaitRunState(t, p, id, 2*time.Second)
	if elapsed := time.Since(start); elapsed < def {
		t.Errorf("run ended after %s, before its %s deadline", elapsed, def)
	}
	assertTimedOut(t, st)
}

// TestTimeoutAboveMaximumIsReduced: a timeout above the plane's maximum is
// reduced to it — the provider sees the maximum, and the run ends there.
func TestTimeoutAboveMaximumIsReduced(t *testing.T) {
	const maxD = 50 * time.Millisecond
	fp := newStubbornAsync(t)
	p := New(Config{Secret: []byte("k"), MaxDeadline: maxD})

	start := time.Now()
	id, err := p.Submit(context.Background(), fp, aei.RunSpec{ID: "too-long", Deadline: time.Hour})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := (<-fp.specs).Deadline; got != maxD {
		t.Errorf("dispatched deadline = %s, want the maximum %s", got, maxD)
	}
	st := awaitRunState(t, p, id, 2*time.Second)
	if elapsed := time.Since(start); elapsed < maxD {
		t.Errorf("run ended after %s, before its %s deadline", elapsed, maxD)
	}
	assertTimedOut(t, st)
}

// TestDeadlineEndsRunWhenProviderIgnoresCancellation: the plane records a
// terminal timeout at the deadline whether the provider is async (Await) or
// blocking (Launch), on both the Submit and the Run path, and a provider result
// that arrives after the deadline does not overwrite it.
func TestDeadlineEndsRunWhenProviderIgnoresCancellation(t *testing.T) {
	const deadline = 50 * time.Millisecond

	t.Run("submit/async", func(t *testing.T) {
		fp := newStubbornAsync(t)
		p := New(Config{Secret: []byte("k")})
		id, _ := p.Submit(context.Background(), fp, aei.RunSpec{ID: "sa", Deadline: deadline})
		<-fp.specs
		assertTimedOut(t, awaitRunState(t, p, id, 2*time.Second))

		// The provider finally reports success; the recorded timeout stands.
		fp.release <- struct{}{}
		time.Sleep(20 * time.Millisecond)
		st, _ := p.Status(id)
		assertTimedOut(t, st)
	})

	t.Run("submit/launch", func(t *testing.T) {
		fp := newStubbornLaunch(t)
		p := New(Config{Secret: []byte("k")})
		id, _ := p.Submit(context.Background(), fp, aei.RunSpec{ID: "sl", Deadline: deadline})
		assertTimedOut(t, awaitRunState(t, p, id, 2*time.Second))
	})

	for name, l := range map[string]func(*testing.T) aei.Launcher{
		"run/async":  func(t *testing.T) aei.Launcher { return newStubbornAsync(t) },
		"run/launch": func(t *testing.T) aei.Launcher { return newStubbornLaunch(t) },
	} {
		t.Run(name, func(t *testing.T) {
			p := New(Config{Secret: []byte("k")})
			resCh := make(chan aei.Result, 1)
			go func() {
				r, _ := p.Run(context.Background(), l(t), aei.RunSpec{ID: name, Deadline: deadline})
				resCh <- r
			}()
			select {
			case r := <-resCh:
				if !errors.Is(r.Err, ErrDeadlineExceeded) {
					t.Errorf("err = %v, want ErrDeadlineExceeded", r.Err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Run did not return at the deadline")
			}
		})
	}
}

// TestCancellationHonoringProviderRecordsDeadline: a provider that does honor
// cancellation reports context.DeadlineExceeded; the plane records its own
// deadline error instead, so a timed-out run reads the same either way.
func TestCancellationHonoringProviderRecordsDeadline(t *testing.T) {
	fp := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	p := New(Config{Secret: []byte("k")})
	r, err := p.Run(context.Background(), fp, aei.RunSpec{ID: "honors", Deadline: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !errors.Is(r.Err, ErrDeadlineExceeded) {
		t.Errorf("err = %v, want ErrDeadlineExceeded", r.Err)
	}
}

// TestRunCallerCancellationIsNotATimeout: cancelling Run's own context ends the
// run with the caller's error, not the deadline error.
func TestRunCallerCancellationIsNotATimeout(t *testing.T) {
	p := New(Config{Secret: []byte("k")})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, _ := p.Run(ctx, newStubbornLaunch(t), aei.RunSpec{ID: "cancelled", Deadline: time.Minute})
	if !errors.Is(r.Err, context.Canceled) || errors.Is(r.Err, ErrDeadlineExceeded) {
		t.Errorf("err = %v, want context.Canceled", r.Err)
	}
}

// TestEffectiveDeadline covers the default and maximum rules, including their
// built-in values and a default configured above the maximum.
func TestEffectiveDeadline(t *testing.T) {
	cases := []struct {
		name      string
		cfg       Config
		requested time.Duration
		want      time.Duration
	}{
		{"unset uses built-in default", Config{}, 0, 15 * time.Minute},
		{"negative uses default", Config{}, -time.Second, 15 * time.Minute},
		{"within max kept", Config{}, 30 * time.Minute, 30 * time.Minute},
		{"above built-in max reduced", Config{}, 2 * time.Hour, time.Hour},
		{"configured default", Config{DefaultDeadline: time.Minute}, 0, time.Minute},
		{"configured max", Config{MaxDeadline: 5 * time.Minute}, 10 * time.Minute, 5 * time.Minute},
		{"default above max is capped", Config{DefaultDeadline: 2 * time.Hour, MaxDeadline: time.Hour}, 0, time.Hour},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := New(c.cfg)
			if got := p.effectiveDeadline(aei.RunSpec{Deadline: c.requested}); got != c.want {
				t.Errorf("effectiveDeadline(%s) = %s, want %s", c.requested, got, c.want)
			}
		})
	}
}
