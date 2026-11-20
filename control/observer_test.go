package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// captureObserver records the runs it observes, safely under concurrency.
type captureObserver struct {
	mu   sync.Mutex
	runs []RunObservation
}

func (c *captureObserver) ObserveRun(o RunObservation) {
	c.mu.Lock()
	c.runs = append(c.runs, o)
	c.mu.Unlock()
}

func (c *captureObserver) snapshot() []RunObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]RunObservation(nil), c.runs...)
}

// waitObserved waits until n runs have been observed. Polling Status is not
// enough on the callback path: /complete stores the terminal state before the
// dispatching goroutine makes the observation.
func (c *captureObserver) waitObserved(t *testing.T, n int) []RunObservation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := c.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d observations, got %d", n, len(c.snapshot()))
	return nil
}

// Submit reports each run's terminal outcome to the Observer exactly once, with its
// taskRef, terminal phase, and the provider-reported timing breakdown.
func TestSubmitObservesTerminalRun(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Observer: obs})

	timing := aei.Timing{ProvisioningSeconds: 2, RuntimeSeconds: 8, ModelSeconds: 5, ModelCalls: 3, ToolCalls: 1}
	fw := &fakeWatch{result: aei.Result{ResultRef: "done", Timing: timing}}
	runID, err := p.Submit(context.Background(), fw, aei.RunSpec{TaskRef: "github-converse"})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, runID)

	got := obs.waitObserved(t, 1)
	if len(got) != 1 {
		t.Fatalf("expected exactly one observation, got %d", len(got))
	}
	if got[0].TaskRef != "github-converse" || got[0].Phase != phaseSucceeded {
		t.Errorf("observed %+v, want taskRef github-converse phase Succeeded", got[0])
	}
	if got[0].Timing != timing {
		t.Errorf("observed timing %+v, want %+v", got[0].Timing, timing)
	}
}

// A run that ends in error is observed with the Failed phase.
func TestSubmitObservesFailedRun(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Observer: obs})

	fw := &fakeWatch{result: aei.Result{Err: errors.New("boom")}}
	runID, err := p.Submit(context.Background(), fw, aei.RunSpec{TaskRef: "github-ingest"})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, runID)

	got := obs.waitObserved(t, 1)
	if len(got) != 1 || got[0].Phase != phaseFailed || got[0].TaskRef != "github-ingest" {
		t.Fatalf("observed %+v, want one Failed github-ingest observation", got)
	}
}

// The default Observer (unset) is a no-op: Submit runs without a metrics sink.
func TestSubmitWithoutObserverIsNoop(t *testing.T) {
	p := New(Config{Secret: []byte("k")})
	runID, err := p.Submit(context.Background(), &fakeWatch{result: aei.Result{ResultRef: "ok"}}, aei.RunSpec{TaskRef: "t"})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, runID)
}

// A run completed by the runtime's /complete callback is observed with the model
// and tool call counts the runtime reported, the provider from its handle, and a
// duration the controller measured itself.
func TestSubmitObservesCallbackCountsAndProvider(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Observer: obs})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	fa := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	runID, err := p.Submit(context.Background(), fa, aei.RunSpec{TaskRef: "github-converse", Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cred := <-fa.dispatched
	time.Sleep(20 * time.Millisecond) // the run takes measurable time before it reports
	resp := post(t, srv.URL+"/aei/v1alpha1/complete", cred.Value, map[string]any{
		"run_id": runID, "result_ref": "res://1",
		"timing": map[string]any{
			"provisioning_seconds": 1.5, "runtime_seconds": 6, "model_seconds": 4,
			"model_calls": 3, "tool_calls": 7,
		},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("complete status = %d, want 202", resp.StatusCode)
	}

	got := obs.waitObserved(t, 1)
	if len(got) != 1 {
		t.Fatalf("expected exactly one observation, got %d", len(got))
	}
	o := got[0]
	want := aei.Timing{ProvisioningSeconds: 1.5, RuntimeSeconds: 6, ModelSeconds: 4, ModelCalls: 3, ToolCalls: 7,
		Measured: aei.StageProvisioning | aei.StageRuntime | aei.StageModel}
	if o.Phase != phaseSucceeded || o.Provider != "fake" || o.Timing != want {
		t.Errorf("observed %+v, want Succeeded, provider fake, timing %+v", o, want)
	}
	if o.Duration < 20*time.Millisecond || o.Duration > 5*time.Second {
		t.Errorf("measured duration = %v, want at least 20ms and under the deadline", o.Duration)
	}
}

// The /complete decoder keeps which durations the runtime sent: an explicit 0 is
// measured, an omitted field is not.
func TestCompleteKeepsDurationPresence(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Observer: obs})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	fa := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	runID, err := p.Submit(context.Background(), fa, aei.RunSpec{TaskRef: "t", Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cred := <-fa.dispatched
	resp := post(t, srv.URL+"/aei/v1alpha1/complete", cred.Value, map[string]any{
		"run_id": runID,
		"timing": map[string]any{"provisioning_seconds": 0, "runtime_seconds": 3, "model_calls": 2},
	})
	resp.Body.Close()

	got := obs.waitObserved(t, 1)[0].Timing
	if want := aei.StageProvisioning | aei.StageRuntime; got.Measured != want {
		t.Errorf("Measured = %b, want provisioning|runtime (%b)", got.Measured, want)
	}
	if got.ModelCalls != 2 || got.RuntimeSeconds != 3 {
		t.Errorf("timing = %+v, want runtime 3, model calls 2", got)
	}
}

// A run that the deadline ends, with no report from the runtime, is observed with
// the duration it really ran, not zero, and with no timing breakdown.
func TestSubmitObservesTimedOutRunDuration(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Observer: obs})

	const deadline = 150 * time.Millisecond
	fa := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	runID, err := p.Submit(context.Background(), fa, aei.RunSpec{TaskRef: "github-review", Deadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	<-fa.dispatched
	waitTerminal(t, p, runID)

	got := obs.waitObserved(t, 1)
	if len(got) != 1 {
		t.Fatalf("expected exactly one observation, got %d", len(got))
	}
	o := got[0]
	if o.Phase != phaseFailed || o.Provider != "fake" {
		t.Errorf("observed %+v, want Failed from provider fake", o)
	}
	if o.Duration < deadline {
		t.Errorf("measured duration = %v, want at least the %v deadline", o.Duration, deadline)
	}
	if o.Timing != (aei.Timing{}) {
		t.Errorf("timing = %+v, want none for a run that never reported", o.Timing)
	}
}

// A synchronous provider's run is attributed to the provider its Launch handle
// names.
func TestSubmitObservesSyncProvider(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Observer: obs})
	runID, err := p.Submit(context.Background(), fakeSync{}, aei.RunSpec{TaskRef: "t"})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, p, runID)
	if got := obs.waitObserved(t, 1); len(got) != 1 || got[0].Provider != "sync-fake" || got[0].Phase != phaseSucceeded {
		t.Fatalf("observed %+v, want one Succeeded run from sync-fake", got)
	}
}

// A synchronous provider whose workload waits on its own /complete call has not
// returned from Launch when the callback wins, so its Handle is not known; the run
// is attributed to the provider the caller named with WithProvider. A provider
// that does report itself in its Handle keeps that name over the fallback.
func TestSubmitProviderFallbackWhenCallbackWins(t *testing.T) {
	obs := &captureObserver{}
	p := New(Config{Secret: []byte("k"), Observer: obs})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	bl := &blockingSync{dispatched: make(chan aei.Credential, 1), release: make(chan struct{})}
	defer close(bl.release)
	runID, err := p.Submit(context.Background(), bl, aei.RunSpec{TaskRef: "t", Deadline: 5 * time.Second}, WithProvider("k8s-job"))
	if err != nil {
		t.Fatal(err)
	}
	cred := <-bl.dispatched
	resp := post(t, srv.URL+"/aei/v1alpha1/complete", cred.Value, map[string]any{"run_id": runID})
	resp.Body.Close()

	got := obs.waitObserved(t, 1)
	if got[0].Provider != "k8s-job" || got[0].Phase != phaseSucceeded {
		t.Fatalf("observed %+v, want Succeeded attributed to k8s-job", got[0])
	}

	// The Handle's own name wins over the fallback.
	fa := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	runID, err = p.Submit(context.Background(), fa, aei.RunSpec{TaskRef: "t", Deadline: 5 * time.Second}, WithProvider("other"))
	if err != nil {
		t.Fatal(err)
	}
	cred = <-fa.dispatched
	resp = post(t, srv.URL+"/aei/v1alpha1/complete", cred.Value, map[string]any{"run_id": runID})
	resp.Body.Close()
	if got := obs.waitObserved(t, 2); got[1].Provider != "fake" {
		t.Fatalf("observed provider %q, want the Handle's fake", got[1].Provider)
	}
}

// blockingSync is a synchronous Launcher whose Launch does not return until the
// test releases it, like a provider that waits for its workload to exit.
type blockingSync struct {
	dispatched chan aei.Credential
	release    chan struct{}
}

func (b *blockingSync) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	b.dispatched <- cred
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return aei.Handle{Provider: "sync-blocked", Ref: spec.ID}, nil
}

// fakeSync is a plain (synchronous) Launcher that returns at once.
type fakeSync struct{}

func (fakeSync) Launch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	return aei.Handle{Provider: "sync-fake", Ref: spec.ID}, nil
}

func waitTerminal(t *testing.T, p *Plane, runID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st, ok := p.Status(runID); ok && st.Phase != phaseRunning {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("run %s did not reach a terminal phase", runID)
}
