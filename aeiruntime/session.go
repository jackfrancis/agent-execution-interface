package aeiruntime

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// This file is the runtime half of durable sessions (docs design sketch): a
// process-global, session-keyed cache of an app-defined "working set" — the
// expensive, reusable state a multi-turn conversation assembles (a fetched corpus,
// a derived index). Under the durable serve loop the process persists, so a
// session's working set survives across its turns (warm); a one-shot runtime starts
// empty and is discarded after the run (always a cold rebuild). Warmth is therefore
// a property of the substrate's process lifetime — which the SDK already selects on
// AEI_SERVE_ADDR — so the same handler runs correctly, differing only in latency, on
// a durable actor (warm) and a per-run workload (cold). Statefulness is a
// performance tier, not a capability the app branches on.

// defaultSessionCacheMax bounds how many distinct sessions' working sets a standing
// actor holds warm at once. It matters only for an actor shared across sessions (a
// per-session actor holds exactly one); an LRU evicts the rest, which just forces a
// rebuild on their next turn. Override with AEI_SESSION_CACHE_MAX.
const defaultSessionCacheMax = 64

// sessions is the process-global working-set cache (see the file comment).
var sessions = newSessionStore(sessionCacheMax())

func sessionCacheMax() int {
	if n, err := strconv.Atoi(os.Getenv("AEI_SESSION_CACHE_MAX")); err == nil && n > 0 {
		return n
	}
	return defaultSessionCacheMax
}

// Session is a handle, scoped to one run's AEI_SESSION, over the process-global
// working-set cache. Obtain it with Runtime.Session and pass it to Working.
type Session struct {
	key   string
	store *sessionStore
	warm  bool
}

// Session returns a handle to this run's session working set. An empty AEI_SESSION
// (a one-shot run) yields a handle that never caches — Working always rebuilds.
func (r *Runtime) Session() *Session {
	return &Session{key: r.spec.Session, store: sessions}
}

// Key returns the session id ("" for a one-shot run).
func (s *Session) Key() string { return s.key }

// Warm reports whether the most recent Working call for this session was served from
// the warm cache (true) rather than rebuilt (false) — an observability signal for
// metrics or a "warm vs rebuilding" UI. Meaningful only after Working returns.
func (s *Session) Warm() bool { return s != nil && s.warm }

// Working returns the session's working set, building it with rebuild on a cold miss
// and reusing the warm copy on a hit — so the same handler is fast on a durable actor
// (warm across turns) and correct-but-cold on a per-run workload (rebuilds each time).
// A session-less run (empty key) always rebuilds. Concurrent turns of one session
// share a single rebuild (single-flight); a failed rebuild is not cached.
//
// The working set is a cache over a reconstructible computation, never the source of
// truth: rebuild MUST reconstruct it from durable inputs (the app's seed), because a
// cold resume, an evicted entry, or a stateless substrate all call it.
func Working[T any](ctx context.Context, s *Session, rebuild func(context.Context) (T, error)) (T, error) {
	if s == nil || s.key == "" {
		return rebuild(ctx) // one-shot: no caching
	}
	v, err, warm := s.store.load(ctx, s.key, func(ctx context.Context) (any, error) {
		return rebuild(ctx)
	})
	s.warm = warm
	if err != nil {
		var zero T
		return zero, err
	}
	t, ok := v.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("aeiruntime: session %q working-set type mismatch (have %T)", s.key, v)
	}
	return t, nil
}

// sessionStore is a bounded, single-flight, process-global cache of session working
// sets keyed by session id, with LRU eviction.
type sessionStore struct {
	mu  sync.Mutex
	max int
	ll  *list.List               // front = most recently used
	m   map[string]*list.Element // key -> *sessionEntry element
}

type sessionEntry struct {
	key   string
	ready chan struct{} // closed once val/err are set (the single-flight barrier)
	val   any
	err   error
}

func newSessionStore(max int) *sessionStore {
	return &sessionStore{max: max, ll: list.New(), m: map[string]*list.Element{}}
}

// load returns the cached working set for key, or builds it with rebuild, reporting
// whether it was already present (warm) vs built by this caller (cold). A concurrent
// caller that finds an in-flight build waits for it and reports warm. A failed
// rebuild is removed so the next turn retries.
func (s *sessionStore) load(ctx context.Context, key string, rebuild func(context.Context) (any, error)) (any, error, bool) {
	s.mu.Lock()
	if el, ok := s.m[key]; ok {
		s.ll.MoveToFront(el)
		e := el.Value.(*sessionEntry)
		s.mu.Unlock()
		<-e.ready // wait out any in-flight build
		return e.val, e.err, true
	}
	// Miss: install a pending entry this caller owns the build for.
	e := &sessionEntry{key: key, ready: make(chan struct{})}
	el := s.ll.PushFront(e)
	s.m[key] = el
	s.evictLocked()
	s.mu.Unlock()

	e.val, e.err = rebuild(ctx)
	close(e.ready)

	if e.err != nil {
		s.mu.Lock()
		if s.m[key] == el { // only remove our own entry
			delete(s.m, key)
			s.ll.Remove(el)
		}
		s.mu.Unlock()
	}
	return e.val, e.err, false
}

// evictLocked drops least-recently-used entries past the cap. The just-inserted entry
// is at the front, so it is never the one evicted. Caller holds s.mu.
func (s *sessionStore) evictLocked() {
	for len(s.m) > s.max {
		back := s.ll.Back()
		if back == nil {
			return
		}
		e := back.Value.(*sessionEntry)
		delete(s.m, e.key)
		s.ll.Remove(back)
	}
}
