package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// captureObserver records the runs it observes, safely under concurrency.
type captureObserver struct {
	mu   sync.Mutex
	runs []observed
}

type observed struct {
	taskRef string
	phase   string
	timing  aei.Timing
}

func (c *captureObserver) ObserveRun(taskRef, phase string, t aei.Timing) {
	c.mu.Lock()
	c.runs = append(c.runs, observed{taskRef, phase, t})
	c.mu.Unlock()
}

func (c *captureObserver) snapshot() []observed {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]observed(nil), c.runs...)
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

	got := obs.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected exactly one observation, got %d", len(got))
	}
	if got[0].taskRef != "github-converse" || got[0].phase != phaseSucceeded {
		t.Errorf("observed %+v, want taskRef github-converse phase Succeeded", got[0])
	}
	if got[0].timing != timing {
		t.Errorf("observed timing %+v, want %+v", got[0].timing, timing)
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

	got := obs.snapshot()
	if len(got) != 1 || got[0].phase != phaseFailed || got[0].taskRef != "github-ingest" {
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
