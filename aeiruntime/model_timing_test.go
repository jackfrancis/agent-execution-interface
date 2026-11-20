package aeiruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// report is one /complete body as the control plane sees it. Timing keeps the raw
// keys so a test can tell an omitted duration from a measured zero.
type report struct {
	RunID  string                     `json:"run_id"`
	Error  string                     `json:"error"`
	Timing map[string]json.RawMessage `json:"timing"`
}

// reportControl is a control plane that records every /complete body, signals
// each on arrived, and holds a run's response while hold[runID] is open, so a
// test can observe what the actor does before its report is acknowledged.
type reportControl struct {
	srv     *httptest.Server
	arrived chan report

	mu      sync.Mutex
	reports []report
	hold    map[string]chan struct{}
}

func newReportControl(t *testing.T) *reportControl {
	t.Helper()
	c := &reportControl{arrived: make(chan report, 16), hold: map[string]chan struct{}{}}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/complete" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var rep report
		if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.mu.Lock()
		c.reports = append(c.reports, rep)
		hold := c.hold[rep.RunID]
		c.mu.Unlock()
		c.arrived <- rep
		if hold != nil {
			<-hold
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// reportsFor returns every /complete body received for runID.
func (c *reportControl) reportsFor(runID string) []report {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []report
	for _, r := range c.reports {
		if r.RunID == runID {
			out = append(out, r)
		}
	}
	return out
}

// onlyReport fails the test unless exactly one report arrived for runID.
func (c *reportControl) onlyReport(t *testing.T, runID string) report {
	t.Helper()
	reps := c.reportsFor(runID)
	if len(reps) != 1 {
		t.Fatalf("%s: got %d completion reports, want exactly 1: %+v", runID, len(reps), reps)
	}
	return reps[0]
}

// wantNumber asserts timing[key] is present and equal to want.
func wantNumber(t *testing.T, timing map[string]json.RawMessage, key string, want float64) {
	t.Helper()
	raw, ok := timing[key]
	if !ok {
		t.Errorf("timing.%s omitted, want %v (timing %s)", key, want, rawTiming(timing))
		return
	}
	var got float64
	if err := json.Unmarshal(raw, &got); err != nil || got != want {
		t.Errorf("timing.%s = %s, want %v", key, raw, want)
	}
}

// wantAbsent asserts timing[key] was not sent.
func wantAbsent(t *testing.T, timing map[string]json.RawMessage, key string) {
	t.Helper()
	if raw, ok := timing[key]; ok {
		t.Errorf("timing.%s = %s, want it omitted", key, raw)
	}
}

func rawTiming(timing map[string]json.RawMessage) string {
	b, _ := json.Marshal(timing)
	return string(b)
}

// TestOneShotReportsModelTiming: model timing an app records with SetModelTiming
// rides the SDK's single one-shot report, alongside its own runtime timing; a
// later call replaces an earlier one.
func TestOneShotReportsModelTiming(t *testing.T) {
	cp := newReportControl(t)
	code := runOneShot(oneShotEnv(cp.srv.URL, time.Now().Add(time.Minute)), nil, func(_ context.Context, rt *Runtime) error {
		rt.SetModelTiming(1, 1, 1)
		rt.SetModelTiming(2.5, 4, 3) // a running total replaces the earlier value
		return nil
	})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	rep := cp.onlyReport(t, "run-oneshot")
	wantNumber(t, rep.Timing, "model_seconds", 2.5)
	wantNumber(t, rep.Timing, "model_calls", 4)
	wantNumber(t, rep.Timing, "tool_calls", 3)
	if _, ok := rep.Timing["runtime_seconds"]; !ok {
		t.Errorf("runtime_seconds omitted; the SDK's own timing must still be sent (timing %s)", rawTiming(rep.Timing))
	}
}

// TestOneShotReportsMeasuredZeroModelTiming: a run that recorded no model use
// still reports model_seconds 0, which the control plane counts as a sample,
// while a run that never called SetModelTiming omits it.
func TestOneShotReportsMeasuredZeroModelTiming(t *testing.T) {
	cp := newReportControl(t)
	if code := runOneShot(oneShotEnv(cp.srv.URL, time.Now().Add(time.Minute)), nil, func(_ context.Context, rt *Runtime) error {
		rt.SetModelTiming(0, 0, 0)
		return nil
	}); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	wantNumber(t, cp.onlyReport(t, "run-oneshot").Timing, "model_seconds", 0)

	unset := newReportControl(t)
	if code := runOneShot(oneShotEnv(unset.srv.URL, time.Now().Add(time.Minute)), nil, func(context.Context, *Runtime) error {
		return nil
	}); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	wantAbsent(t, unset.onlyReport(t, "run-oneshot").Timing, "model_seconds")
}

// TestOneShotModelTimingOnFailure: a failed run's report carries the model
// timing too, and the process still exits non-zero.
func TestOneShotModelTimingOnFailure(t *testing.T) {
	cp := newReportControl(t)
	code := runOneShot(oneShotEnv(cp.srv.URL, time.Now().Add(time.Minute)), nil, func(_ context.Context, rt *Runtime) error {
		rt.SetModelTiming(0.75, 2, 0)
		return context.DeadlineExceeded
	})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	rep := cp.onlyReport(t, "run-oneshot")
	if rep.Error == "" {
		t.Error("report carries no error for a failed run")
	}
	wantNumber(t, rep.Timing, "model_seconds", 0.75)
	wantNumber(t, rep.Timing, "model_calls", 2)
}

// TestSetModelTimingRejectsNonMeasurements: seconds that are negative, NaN, or
// infinite leave model_seconds omitted, so a bad value cannot make the report
// unencodable (and so lose the run's outcome); negative counts are sent as 0.
func TestSetModelTimingRejectsNonMeasurements(t *testing.T) {
	for _, tc := range []struct {
		name        string
		seconds     float64
		calls, tool int
		want        Timing
	}{
		{"NaN", math.NaN(), 3, 1, Timing{ModelCalls: 3, ToolCalls: 1}},
		{"+Inf", math.Inf(1), 3, 1, Timing{ModelCalls: 3, ToolCalls: 1}},
		{"-Inf", math.Inf(-1), 3, 1, Timing{ModelCalls: 3, ToolCalls: 1}},
		{"negative", -1, 3, 1, Timing{ModelCalls: 3, ToolCalls: 1}},
		{"negative counts", 2, -1, -5, Timing{ModelSeconds: 2, Measured: StageModel}},
	} {
		rt := &Runtime{}
		rt.SetModelTiming(tc.seconds, tc.calls, tc.tool)
		var got Timing
		rt.applyModelTiming(&got)
		if got != tc.want {
			t.Errorf("%s: timing = %+v, want %+v", tc.name, got, tc.want)
		}
	}

	// End to end: a NaN still lets the report land.
	cp := newReportControl(t)
	if code := runOneShot(oneShotEnv(cp.srv.URL, time.Now().Add(time.Minute)), nil, func(_ context.Context, rt *Runtime) error {
		rt.SetModelTiming(math.NaN(), 1, 0)
		return nil
	}); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	rep := cp.onlyReport(t, "run-oneshot")
	wantAbsent(t, rep.Timing, "model_seconds")
	wantNumber(t, rep.Timing, "model_calls", 1)
}

// TestSetModelTimingConcurrent: SetModelTiming is safe from any goroutine,
// including one still running after fn returns (run with -race).
func TestSetModelTimingConcurrent(t *testing.T) {
	rt := &Runtime{spec: RunSpec{RunID: "run-race"}}
	stop := make(chan struct{})
	var straggler sync.WaitGroup
	timing, err := runTask(context.Background(), rt, func(_ context.Context, rt *Runtime) error {
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() { rt.SetModelTiming(float64(i), i, i) })
		}
		wg.Wait()
		straggler.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					rt.SetModelTiming(99, 99, 99)
				}
			}
		})
		return nil
	})
	close(stop)
	straggler.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if timing.Measured&StageModel == 0 {
		t.Errorf("model stage not measured: %+v", timing)
	}
}

// postTask posts a message/send for runID to the actor and returns the HTTP
// status and, on a 200, the ack's task state.
func postTask(t *testing.T, url, runID, controlURL string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": runID, "method": methodSendTask,
		"params": map[string]any{"message": map[string]any{"metadata": map[string]string{
			EnvRunID: runID, EnvControlURL: controlURL, EnvCredential: "push-tok",
		}}},
	})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", runID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, ""
	}
	var out rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.Result == nil {
		t.Fatalf("post %s: undecodable ack (%v)", runID, err)
	}
	return resp.StatusCode, out.Result.Status.State
}

// TestServeReportsModelTimingOnceAndFreesSlot: on a capacity-one durable actor,
// the SDK sends one report per run carrying the app's model timing; the run's
// slot is held until the control plane acknowledges that report and freed as
// soon as it does, so the next task is accepted rather than shed with a 503.
func TestServeReportsModelTimingOnceAndFreesSlot(t *testing.T) {
	cp := newReportControl(t)
	ack := make(chan struct{})
	cp.mu.Lock()
	cp.hold["run-1"] = ack
	cp.mu.Unlock()
	fn := func(_ context.Context, rt *Runtime) error {
		if rt.Spec().RunID == "run-1" {
			rt.SetModelTiming(3.25, 5, 7)
		} else {
			rt.SetModelTiming(0, 0, 0) // a durable run with no model use
		}
		return nil
	}
	actor := httptest.NewServer(actorMux(func(string) string { return "" }, fn, newSemaphore(1)))
	defer actor.Close()

	if code, state := postTask(t, actor.URL, "run-1", cp.srv.URL); code != http.StatusOK || state != stateSubmitted {
		t.Fatalf("run-1 ack = %d %q, want 200 %q", code, state, stateSubmitted)
	}
	select {
	case <-cp.arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("run-1 never reported")
	}

	// The report is in flight but unacknowledged: the run still holds its slot.
	if code, _ := postTask(t, actor.URL, "run-2", cp.srv.URL); code != http.StatusServiceUnavailable {
		t.Fatalf("run-2 before run-1's report was acknowledged: status %d, want 503", code)
	}

	close(ack)
	acked := time.Now()
	for {
		code, state := postTask(t, actor.URL, "run-2", cp.srv.URL)
		if code == http.StatusOK && state == stateSubmitted {
			break
		}
		if code != http.StatusServiceUnavailable {
			t.Fatalf("run-2 ack = %d %q, want 200 %q", code, state, stateSubmitted)
		}
		if time.Since(acked) > time.Second {
			t.Fatal("run-1's slot was not freed within 1s of its report being acknowledged")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-cp.arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("run-2 never reported")
	}

	time.Sleep(100 * time.Millisecond) // room for a stray duplicate to show up
	rep1 := cp.onlyReport(t, "run-1")
	wantNumber(t, rep1.Timing, "model_seconds", 3.25)
	wantNumber(t, rep1.Timing, "model_calls", 5)
	wantNumber(t, rep1.Timing, "tool_calls", 7)
	wantNumber(t, cp.onlyReport(t, "run-2").Timing, "model_seconds", 0)
}
