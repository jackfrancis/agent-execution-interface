package control

import (
	"sync"
	"time"
)

// Store holds the control plane's persistable state: single-use redemption tickets
// and observable run status. It is the seam that decides how far the plane scales.
//
// The reference implementation (memStore) is in-memory and single-process, which is
// why the controller Deployment pins replicas: 1 — a callback, a ticket redemption,
// or a status read must reach the same process that armed the run. Supplying a shared
// implementation (Redis, SQL, etcd) makes that state cross-replica and is what lets
// the control plane run replicas > 1: the completion waiter (Plane.waiters) stays a
// same-replica latency optimization, but correctness comes from this store, since a
// /complete callback landing on ANY replica records the terminal outcome durably and
// FinishRun is first-writer-wins (rule R4). This is the seam the HA profile plugs into
// (roadmap Phase 7). Implementations MUST be safe for concurrent use.
type Store interface {
	// PutTicket stores a single-use ticket's claims under id until expires.
	PutTicket(id string, c Claims, expires time.Time)
	// RedeemTicket atomically consumes ticket id and returns its claims, or false if
	// it is unknown, already redeemed, or expired at now. The single-use guarantee
	// (redeem-once) MUST hold across concurrent callers — the security property a
	// pull credential rests on.
	RedeemTicket(id string, now time.Time) (Claims, bool)

	// SetRunning records a run as in progress. It MUST NOT revert a run that is
	// already terminal (a callback may beat the dispatcher's own bookkeeping).
	SetRunning(runID string)
	// FinishRun records a run's terminal outcome and reports whether it wrote. It is
	// first-writer-wins: once a run is terminal, later calls are ignored and return
	// false, so a runtime callback and a watch that both fire converge to one
	// outcome and one log line (rule R4).
	FinishRun(runID string, st RunState) bool
	// Run returns a run's state and whether it is known.
	Run(runID string) (RunState, bool)
}

// memStore is the reference-grade Store: in-memory maps guarded by one mutex, scoped
// to a single process. It does not evict, so a long-lived plane accumulates one entry
// per run and ticket — acceptable for the reference, where a production Store adds TTL
// and eviction along with cross-replica durability.
type memStore struct {
	mu      sync.Mutex
	tickets map[string]storedTicket
	runs    map[string]RunState
}

type storedTicket struct {
	claims  Claims
	expires time.Time
}

func newMemStore() *memStore {
	return &memStore{tickets: map[string]storedTicket{}, runs: map[string]RunState{}}
}

func (s *memStore) PutTicket(id string, c Claims, expires time.Time) {
	s.mu.Lock()
	s.tickets[id] = storedTicket{claims: c, expires: expires}
	s.mu.Unlock()
}

func (s *memStore) RedeemTicket(id string, now time.Time) (Claims, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tickets[id]
	if !ok {
		return Claims{}, false
	}
	delete(s.tickets, id) // single-use: consume whether or not it is still valid
	if now.After(t.expires) {
		return Claims{}, false
	}
	return t.claims, true
}

func (s *memStore) SetRunning(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.runs[runID]; ok && st.Phase != phaseRunning {
		return // already terminal; do not revert
	}
	s.runs[runID] = RunState{Phase: phaseRunning}
}

func (s *memStore) FinishRun(runID string, st RunState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.runs[runID]; ok && existing.Phase != phaseRunning {
		return false // first terminal outcome wins (R4)
	}
	s.runs[runID] = st
	return true
}

func (s *memStore) Run(runID string) (RunState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.runs[runID]
	return st, ok
}
