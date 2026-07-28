package aeiruntime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"
)

// The A2A task states the durable actor reports on the synchronous ack. The
// agent-substrate provider treats stateFailed as a pre-run rejection (a malformed
// or unauthorized request) and any other state as accepted — the run then proceeds
// detached and reports its terminal outcome out-of-band via Complete (the callback
// the control plane races against the provider's deadline backstop). So the ack
// only distinguishes "rejected now" from "accepted, running".
const (
	stateSubmitted = "submitted" // accepted; running detached
	stateFailed    = "failed"    // rejected synchronously, before running

	methodSendTask = "message/send" // the only A2A method the actor serves
)

// serve runs the runtime as a long-lived durable actor. It is the mirror of the
// agent-substrate provider's dispatch client: that provider POSTs one JSON-RPC A2A
// message/send per run through the atenet-router, which resumes this actor and
// proxies to it; serve receives each message, decodes the run from the message
// metadata (rule R1 — per-run keys ride the dispatch, not the frozen actor env),
// redeems the pull-ticket, and runs fn — the exact one-shot Main bracket, once per
// task. It blocks until ctx is cancelled, then drains in-flight connections.
//
// serve is unexported on purpose: an app never chooses it. Main selects it when the
// launcher sets AEI_SERVE_ADDR, so one substrate-agnostic main() serves both the
// workload and durable substrates without conditional application code.
func serve(ctx context.Context, addr string, getenv func(string) string, fn func(context.Context, *Runtime) error) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           actorMux(getenv, fn, newSemaphore(maxConcurrentRuns(getenv))),
		ReadHeaderTimeout: 10 * time.Second,
	}
	// Shut down gracefully when the process is asked to stop. In-flight runs are on
	// their own (Background) contexts bounded by each run's deadline, so a shutdown
	// stops accepting new tasks without cancelling work already dispatched.
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// actorMux builds the durable actor's routes. POST / is the A2A JSON-RPC endpoint
// the router proxies to (routing is by Host, so the path is just "/"); a GET
// answers readiness/liveness probes so the router can tell a resumed actor is live.
// sem bounds how many tasks run concurrently (nil = unbounded).
func actorMux(getenv func(string) string, fn func(context.Context, *Runtime) error, sem *semaphore) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK) // readiness/liveness probe
		case http.MethodPost:
			handleTask(w, r, getenv, fn, sem)
		default:
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	return mux
}

// taskRequest is the subset of the JSON-RPC A2A message/send the actor decodes.
// The run's AEI_* keys ride the message metadata, mirroring aei.Injection on the
// dispatch side — the two halves meet at the wire, not in shared Go types.
type taskRequest struct {
	ID     json.RawMessage `json:"id"` // echoed back; may be a string or a number
	Method string          `json:"method"`
	Params struct {
		Message struct {
			Metadata map[string]string `json:"metadata"`
		} `json:"message"`
	} `json:"params"`
}

// handleTask decodes one task, redeems its ticket, acks acceptance, then runs fn
// detached and reports completion. Load (including the ticket redemption) runs
// synchronously so a malformed or unauthorized request is rejected in the ack
// (stateFailed) rather than surfacing only as a late completion error. A valid
// request is acked as accepted and the run proceeds on its own context, bounded by
// the run's deadline, independent of this HTTP request's lifetime.
//
// sem bounds concurrent runs (nil = unbounded). A slot is acquired BEFORE Load so a
// shed task never redeems (and spends) its single-use ticket — the request stays
// safely retryable. At capacity the actor replies 503: a retryable signal the
// provider's warm-up retry already absorbs (the same signal as a cold-start "no
// free workers"), so overload and cold-start share one back-off path and the
// application handler never learns it was throttled.
func handleTask(w http.ResponseWriter, r *http.Request, getenv func(string) string, fn func(context.Context, *Runtime) error, sem *semaphore) {
	var req taskRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
		writeRPCError(w, nil, -32700, "parse error: "+err.Error())
		return
	}
	if req.Method != methodSendTask {
		writeRPCError(w, req.ID, -32601, "method not found: "+req.Method)
		return
	}

	// Admission control: acquire a run slot before redeeming the ticket. On refusal
	// the ticket is untouched, so the 503 is safe to retry.
	if !sem.tryAcquire() {
		http.Error(w, "actor at capacity", http.StatusServiceUnavailable)
		return
	}

	// Decode the task (metadata-first) and redeem its ticket now, so a bad request
	// is rejected synchronously — we never accept a task we cannot run.
	run, err := Load(r.Context(), getenv, req.Params.Message.Metadata)
	if err != nil {
		sem.release() // never ran; hand the slot back
		writeTask(w, req.ID, req.Params.Message.Metadata[EnvRunID], stateFailed, err.Error())
		return
	}

	// Accepted: run detached and report completion out-of-band. Ack immediately —
	// the provider dispatches non-blocking and drives completion off the callback.
	go func() {
		defer sem.release()
		timing, runErr := runTask(context.Background(), run, fn)
		reportCompletion(run, runErr, timing)
	}()
	writeTask(w, req.ID, run.spec.RunID, stateSubmitted, "")
}

// rpcResponse is the JSON-RPC 2.0 envelope the actor returns. Per JSON-RPC, both
// results and errors ride an HTTP 200 (the provider checks for 200, then decodes
// result vs error), so writeRPC always writes 200.
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  *a2aTask        `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type a2aTask struct {
	ID     string    `json:"id"`
	Kind   string    `json:"kind"`
	Status a2aStatus `json:"status"`
}

type a2aStatus struct {
	State   string      `json:"state"`
	Message *a2aMessage `json:"message,omitempty"`
}

type a2aMessage struct {
	Role  string    `json:"role"`
	Kind  string    `json:"kind"`
	Parts []a2aPart `json:"parts"`
}

type a2aPart struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// writeTask returns the task ack: stateSubmitted (accepted, running detached) or
// stateFailed (rejected). A note is attached as an agent message part only when
// non-empty — the provider surfaces it on rejection.
func writeTask(w http.ResponseWriter, id json.RawMessage, taskID, state, note string) {
	t := &a2aTask{ID: taskID, Kind: "task", Status: a2aStatus{State: state}}
	if note != "" {
		t.Status.Message = &a2aMessage{Role: "agent", Kind: "message", Parts: []a2aPart{{Kind: "text", Text: note}}}
	}
	writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Result: t})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}})
}

func writeRPC(w http.ResponseWriter, resp rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// semaphore is a fixed-capacity admission gate for concurrent runs. A nil
// *semaphore is unbounded — tryAcquire always succeeds and release is a no-op — so
// the unset/uncapped path adds no synchronization.
type semaphore struct{ slots chan struct{} }

// newSemaphore returns a semaphore of capacity n, or nil (unbounded) when n <= 0.
func newSemaphore(n int) *semaphore {
	if n <= 0 {
		return nil
	}
	return &semaphore{slots: make(chan struct{}, n)}
}

// tryAcquire takes a slot without blocking, reporting whether one was free.
func (s *semaphore) tryAcquire() bool {
	if s == nil {
		return true
	}
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release returns a slot taken by tryAcquire.
func (s *semaphore) release() {
	if s == nil {
		return
	}
	<-s.slots
}

// maxConcurrentRuns reads the serve-loop concurrency cap from AEI_MAX_CONCURRENT_RUNS.
// Unset, non-numeric, or <= 0 means unbounded (0), preserving the uncapped default.
func maxConcurrentRuns(getenv func(string) string) int {
	n, err := strconv.Atoi(getenv(EnvMaxConcurrent))
	if err != nil || n < 0 {
		return 0
	}
	return n
}
