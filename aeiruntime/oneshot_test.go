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

// TestOneShotReportBudget: a one-shot workload stops retrying in time to exit
// before its deadline, and always gets at least one prompt attempt.
func TestOneShotReportBudget(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name     string
		deadline time.Time
		want     time.Duration
	}{
		{"no deadline", time.Time{}, oneShotCompletionBudget},
		{"distant deadline", now.Add(10 * time.Minute), oneShotCompletionBudget},
		{"near deadline", now.Add(15 * time.Second), 15*time.Second - oneShotExitMargin},
		{"inside the exit margin", now.Add(3 * time.Second), oneShotMinBudget},
		{"past the deadline", now.Add(-time.Minute), oneShotMinBudget},
	}
	for _, c := range cases {
		if got := oneShotReportBudget(c.deadline, now); got != c.want {
			t.Errorf("%s: budget = %v, want %v", c.name, got, c.want)
		}
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
