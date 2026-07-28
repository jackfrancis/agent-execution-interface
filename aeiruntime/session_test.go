package aeiruntime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustLoad(t *testing.T, md map[string]string) *Runtime {
	t.Helper()
	rt, err := Load(context.Background(), func(string) string { return "" }, md)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return rt
}

// TestWorkingWarmAcrossTurns: a session's working set is built once and reused on the
// next turn in the same process (the durable-serve-loop case), and reports warm.
func TestWorkingWarmAcrossTurns(t *testing.T) {
	builds := 0
	build := func(context.Context) (int, error) { builds++; return 42, nil }

	rt1 := mustLoad(t, map[string]string{EnvRunID: "r1", EnvControlURL: "http://cp", EnvSession: "warm-A"})
	if v, err := Working(context.Background(), rt1.Session(), build); err != nil || v != 42 {
		t.Fatalf("turn 1 = %v, %v", v, err)
	}

	rt2 := mustLoad(t, map[string]string{EnvRunID: "r2", EnvControlURL: "http://cp", EnvSession: "warm-A"})
	s2 := rt2.Session()
	if v, err := Working(context.Background(), s2, build); err != nil || v != 42 {
		t.Fatalf("turn 2 = %v, %v", v, err)
	}
	if builds != 1 {
		t.Fatalf("builds = %d, want 1 (turn 2 reuses the warm working set)", builds)
	}
	if !s2.Warm() {
		t.Error("turn 2 should report warm")
	}
}

// TestWorkingOneShotAlwaysRebuilds: with no session (a one-shot run) there is no
// caching — each call rebuilds, and Warm is always false.
func TestWorkingOneShotAlwaysRebuilds(t *testing.T) {
	rt := mustLoad(t, map[string]string{EnvRunID: "r", EnvControlURL: "http://cp"}) // no AEI_SESSION
	builds := 0
	build := func(context.Context) (int, error) { builds++; return 1, nil }

	_, _ = Working(context.Background(), rt.Session(), build)
	s := rt.Session()
	_, _ = Working(context.Background(), s, build)
	if builds != 2 {
		t.Fatalf("builds = %d, want 2 (no session ⇒ no caching)", builds)
	}
	if s.Warm() {
		t.Error("a one-shot session is never warm")
	}
}

// TestWorkingErrorNotCached: a failed rebuild is not cached, so the next turn retries.
func TestWorkingErrorNotCached(t *testing.T) {
	calls := 0
	build := func(context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, fmt.Errorf("transient")
		}
		return 7, nil
	}
	rt := mustLoad(t, map[string]string{EnvRunID: "r", EnvControlURL: "http://cp", EnvSession: "err-A"})
	if _, err := Working(context.Background(), rt.Session(), build); err == nil {
		t.Fatal("turn 1 should surface the rebuild error")
	}
	v, err := Working(context.Background(), rt.Session(), build)
	if err != nil || v != 7 {
		t.Fatalf("turn 2 = %v, %v — a failed rebuild must not be cached", v, err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

// TestSessionStoreSingleFlight: concurrent turns of one session share one rebuild.
func TestSessionStoreSingleFlight(t *testing.T) {
	store := newSessionStore(8)
	var builds int32
	release := make(chan struct{})
	build := func(context.Context) (any, error) {
		atomic.AddInt32(&builds, 1)
		<-release // hold the build open so all callers pile onto it
		return "X", nil
	}

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, _ = store.load(context.Background(), "k", build) }()
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&builds); got != 1 {
		t.Fatalf("builds = %d, want 1 (single-flight)", got)
	}
}

// TestSessionStoreEviction: past the cap, the least-recently-used session is evicted.
func TestSessionStoreEviction(t *testing.T) {
	store := newSessionStore(2)
	build := func(k string) func(context.Context) (any, error) {
		return func(context.Context) (any, error) { return k, nil }
	}
	store.load(context.Background(), "a", build("a"))
	store.load(context.Background(), "b", build("b"))
	store.load(context.Background(), "c", build("c")) // evicts LRU "a"

	if _, ok := store.m["a"]; ok {
		t.Error("a should have been evicted")
	}
	if _, ok := store.m["b"]; !ok {
		t.Error("b should remain")
	}
	if _, ok := store.m["c"]; !ok {
		t.Error("c should remain")
	}
}
