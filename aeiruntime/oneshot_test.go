package aeiruntime

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestOneShotReportPlan: a one-shot workload stops retrying in time to exit
// before its deadline, and inside the exit margin makes one brief attempt only.
func TestOneShotReportPlan(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name      string
		deadline  time.Time
		want      time.Duration
		wantRetry bool
	}{
		{"no deadline", time.Time{}, oneShotCompletionBudget, true},
		{"distant deadline", now.Add(10 * time.Minute), oneShotCompletionBudget, true},
		{"near deadline", now.Add(15 * time.Second), 15*time.Second - oneShotExitMargin, true},
		{"window exactly one final attempt", now.Add(oneShotExitMargin + oneShotFinalAttempt), oneShotFinalAttempt, true},
		// 20ms of window would starve a healthy controller; one full attempt instead.
		{"window shorter than one attempt", now.Add(oneShotExitMargin + 20*time.Millisecond), oneShotFinalAttempt, false},
		{"inside the exit margin", now.Add(3 * time.Second), oneShotFinalAttempt, false},
		{"past the deadline", now.Add(-time.Minute), oneShotFinalAttempt, false},
	}
	for _, c := range cases {
		got, retry := oneShotReportPlan(c.deadline, now)
		if got != c.want || retry != c.wantRetry {
			t.Errorf("%s: plan = (%v, retry %v), want (%v, retry %v)", c.name, got, retry, c.want, c.wantRetry)
		}
	}
}

// failingControl is a control plane whose /complete always answers 503.
func failingControl(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	return srv, &attempts
}

func oneShotEnv(controlURL string, deadline time.Time) func(string) string {
	env := map[string]string{
		EnvRunID:      "run-oneshot",
		EnvCredential: "push-tok",
		EnvControlURL: controlURL,
		EnvDeadline:   strconv.FormatInt(deadline.UnixMilli(), 10),
	}
	return func(k string) string { return env[k] }
}

// TestOneShotExplicitCompleteRespectsExitMargin: a workload that calls Complete
// itself, to publish a result ref, is held to the same exit margin as the SDK's
// own report, so it still exits in time for the watch to see success.
func TestOneShotExplicitCompleteRespectsExitMargin(t *testing.T) {
	srv, attempts := failingControl(t)
	deadline := time.Now().Add(oneShotExitMargin + 2*time.Second)
	code := runOneShot(oneShotEnv(srv.URL, deadline), nil, func(_ context.Context, rt *Runtime) error {
		// context.Background: the app sets no bound of its own.
		_ = rt.Complete(context.Background(), nil, Timing{}, "res://published")
		return nil
	})
	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if latest := deadline.Add(-oneShotExitMargin + time.Second); time.Now().After(latest) {
		t.Errorf("exited %v after the exit margin began; want before it", time.Since(deadline.Add(-oneShotExitMargin)))
	}
	if attempts.Load() < 2 {
		t.Errorf("attempts = %d, want the explicit report retried within its budget", attempts.Load())
	}
}

// TestOneShotInsideExitMarginMakesOneAttempt: once inside the exit margin, a
// failed report is not retried.
func TestOneShotInsideExitMarginMakesOneAttempt(t *testing.T) {
	srv, attempts := failingControl(t)
	rt, err := Load(context.Background(), oneShotEnv(srv.URL, time.Now().Add(3*time.Second)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Complete(context.Background(), nil, Timing{}, ""); err == nil {
		t.Fatal("want the 503 returned")
	}
	if attempts.Load() != 1 {
		t.Errorf("attempts = %d, want 1", attempts.Load())
	}
}

// TestOneShotFinalAttemptIsBrief: the single attempt inside the exit margin gives
// up after oneShotFinalAttempt against a controller that never answers.
func TestOneShotFinalAttemptIsBrief(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // runs before srv.Close
	rt, err := Load(context.Background(), oneShotEnv(srv.URL, time.Now().Add(time.Second)), nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := rt.Complete(context.Background(), nil, Timing{}, ""); err == nil {
		t.Fatal("want a timeout error")
	}
	if elapsed := time.Since(start); elapsed > oneShotFinalAttempt+500*time.Millisecond {
		t.Errorf("final attempt took %v, want about %v", elapsed, oneShotFinalAttempt)
	}
}

// TestLoadRejectsUnusableEndpoints: a control or broker URL no request can be
// sent to fails Load, instead of a Complete that retries forever.
func TestLoadRejectsUnusableEndpoints(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{EnvControlURL, "controller.default.svc"},
		{EnvControlURL, "ftp://controller.default.svc"},
		{EnvControlURL, "http://"},
		{EnvControlURL, "http://:8080"},
		{EnvControlURL, "http://controller.default.svc:99999"},
		{EnvControlURL, "http://controller.default.svc:0"},
		{EnvBrokerURL, "broker.default.svc:8080"},
		{EnvCredential, "tok\n"},
		{EnvCredential, "tok\r\nX-Injected: 1"},
	} {
		env := map[string]string{EnvRunID: "r", EnvCredential: "tok", EnvControlURL: "http://cp.default.svc"}
		env[tc.key] = tc.value
		if _, err := Load(context.Background(), func(k string) string { return env[k] }, nil); err == nil {
			t.Errorf("%s=%q: want Load to fail", tc.key, tc.value)
		}
	}
	// Valid forms still load.
	for _, u := range []string{"http://cp.default.svc", "https://cp.example:8443/aei/v1alpha1", "http://[::1]:8080"} {
		env := map[string]string{EnvRunID: "r", EnvCredential: "tok", EnvControlURL: u}
		if _, err := Load(context.Background(), func(k string) string { return env[k] }, nil); err != nil {
			t.Errorf("%s=%q: Load: %v", EnvControlURL, u, err)
		}
	}
}

// TestLoadRejectsUnsendableRedeemedCredential: a credential the broker returns is
// checked the same way as an injected one.
func TestLoadRejectsUnsendableRedeemedCredential(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"credential":"tok\n","expires_in":60}`))
	}))
	defer srv.Close()
	env := map[string]string{EnvRunID: "r", EnvTicket: "ticket", EnvControlURL: srv.URL}
	if _, err := Load(context.Background(), func(k string) string { return env[k] }, nil); err == nil {
		t.Error("want Load to fail for a redeemed credential with a newline")
	}
}

// TestOneShotExitsBeforeDeadlineDuringCallbackOutage: work that succeeds early in
// a run whose /complete keeps failing still exits successfully, and early enough
// for the provider's watch to see the exit before the run deadline.
func TestOneShotExitsBeforeDeadlineDuringCallbackOutage(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	deadline := time.Now().Add(oneShotExitMargin + 2*time.Second)
	env := map[string]string{
		EnvRunID:      "run-oneshot",
		EnvCredential: "push-tok",
		EnvControlURL: srv.URL,
		EnvDeadline:   strconv.FormatInt(deadline.UnixMilli(), 10),
	}
	start := time.Now()
	code := runOneShot(func(k string) string { return env[k] }, nil, func(context.Context, *Runtime) error { return nil })
	if code != 0 {
		t.Errorf("exit code = %d, want 0 for successful work", code)
	}
	if latest := deadline.Add(-oneShotExitMargin + time.Second); time.Now().After(latest) {
		t.Errorf("exited %v after start; want no later than %v before the deadline",
			time.Since(start), oneShotExitMargin-time.Second)
	}
	if attempts.Load() < 2 {
		t.Errorf("attempts = %d, want the report retried within its budget", attempts.Load())
	}
}

// TestCompleteReturnsEncodeErrorImmediately: a request that cannot be encoded
// fails the same way on every attempt, so Complete returns at once, even with no
// deadline on its context, and sends nothing.
func TestCompleteReturnsEncodeErrorImmediately(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	rt := &Runtime{spec: RunSpec{RunID: "run-nan"}, controlURL: srv.URL, cred: "tok", http: srv.Client()}

	done := make(chan error, 1)
	go func() { done <- rt.Complete(context.Background(), nil, Timing{RuntimeSeconds: math.NaN()}, "") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want an encode error for a NaN timing")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Complete kept retrying a request that cannot be encoded")
	}
	if attempts.Load() != 0 {
		t.Errorf("attempts = %d, want 0", attempts.Load())
	}
}

// TestOneShotHealthyCallbackAtExitMarginSucceeds: with just over the exit margin
// left, a healthy controller that takes 100ms to answer still records the
// completion; the run is not starved of a full attempt.
func TestOneShotHealthyCallbackAtExitMarginSucceeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	rt, err := Load(context.Background(), oneShotEnv(srv.URL, time.Now().Add(oneShotExitMargin+20*time.Millisecond)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Complete(context.Background(), nil, Timing{}, "res://kept"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
}
