package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
)

// The collector records terminal runs and exposes them at /metrics, labeled by
// task, phase, and provider, with a per-stage duration breakdown and the
// reported call counts.
func TestCollectorRecordsAndExposes(t *testing.T) {
	c := New()
	c.ObserveRun(control.RunObservation{
		TaskRef: "github-converse", Phase: "Succeeded", Provider: "k8s-job", Duration: 11 * time.Second,
		Timing: aei.Timing{ProvisioningSeconds: 2, RuntimeSeconds: 8, ModelSeconds: 5, ModelCalls: 3, ToolCalls: 4,
			Measured: aei.StageProvisioning | aei.StageRuntime | aei.StageModel},
	})
	c.ObserveRun(control.RunObservation{
		TaskRef: "github-converse", Phase: "Failed", Provider: "k8s-job", Duration: 3 * time.Second,
		Timing: aei.Timing{ProvisioningSeconds: 1, RuntimeSeconds: 1, Measured: aei.StageProvisioning | aei.StageRuntime},
	})

	body := scrape(t, c.Handler())
	for _, want := range []string{
		`aei_runs_total{phase="Succeeded",provider="k8s-job",task_ref="github-converse"} 1`,
		`aei_runs_total{phase="Failed",provider="k8s-job",task_ref="github-converse"} 1`,
		`aei_run_model_calls_total{phase="Succeeded",provider="k8s-job",task_ref="github-converse"} 3`,
		`aei_run_tool_calls_total{phase="Succeeded",provider="k8s-job",task_ref="github-converse"} 4`,
		`aei_run_duration_seconds_sum{phase="Succeeded",provider="k8s-job",stage="total",task_ref="github-converse"} 11`,
		`aei_run_duration_seconds_sum{phase="Succeeded",provider="k8s-job",stage="provisioning",task_ref="github-converse"} 2`,
		`aei_run_duration_seconds_sum{phase="Succeeded",provider="k8s-job",stage="runtime",task_ref="github-converse"} 8`,
		`aei_run_duration_seconds_sum{phase="Succeeded",provider="k8s-job",stage="model",task_ref="github-converse"} 5`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
	// The failed run's report had no model share, so it adds no model observation.
	if strings.Contains(body, `phase="Failed",provider="k8s-job",stage="model"`) {
		t.Errorf("a report without model time should not record the model stage\n%s", body)
	}
}

// A run that ended without a timing report records its measured total and
// nothing else, so the reported stages are not dragged toward zero.
func TestCollectorUnreportedRunRecordsTotalOnly(t *testing.T) {
	c := New()
	c.ObserveRun(control.RunObservation{TaskRef: "github-review", Phase: "Failed", Provider: "agent-sandbox", Duration: 90 * time.Second})

	body := scrape(t, c.Handler())
	if want := `aei_run_duration_seconds_sum{phase="Failed",provider="agent-sandbox",stage="total",task_ref="github-review"} 90`; !strings.Contains(body, want) {
		t.Errorf("metrics missing %q\n%s", want, body)
	}
	for _, stage := range []string{"provisioning", "runtime", "model"} {
		if strings.Contains(body, `stage="`+stage+`"`) {
			t.Errorf("unreported run should not record stage %q\n%s", stage, body)
		}
	}
}

// Every timing field is optional: a report with call counts but no model time
// counts the calls without a zero-second model observation, and a report with no
// stage times records only the controller's total.
func TestCollectorRecordsOnlyReportedStages(t *testing.T) {
	c := New()
	c.ObserveRun(control.RunObservation{
		TaskRef: "github-review", Phase: "Succeeded", Provider: "k8s-job", Duration: 4 * time.Second,
		Timing: aei.Timing{ModelCalls: 3, ToolCalls: 2},
	})
	f := gather(t, c)
	l := map[string]string{"task_ref": "github-review", "phase": "Succeeded", "provider": "k8s-job"}
	if got, _ := f.value("aei_run_model_calls_total", l); got != 3 {
		t.Errorf("model calls = %v, want 3", got)
	}
	if got, _ := f.value("aei_run_tool_calls_total", l); got != 2 {
		t.Errorf("tool calls = %v, want 2", got)
	}
	for _, s := range []string{"provisioning", "runtime", "model"} {
		if _, ok := f.value("aei_run_duration_seconds", withStage(l, s)); ok {
			t.Errorf("stage %q recorded without a reported value", s)
		}
	}
	if got, _ := f.value("aei_run_duration_seconds", withStage(l, "total")); got != 4 {
		t.Errorf("total = %v, want 4", got)
	}
}

// A malformed report with negative counts must not panic the controller (the
// observer runs on Submit's goroutine and Counter.Add panics on a negative value);
// the run and the rest of its report are still recorded.
func TestCollectorIgnoresNegativeCounts(t *testing.T) {
	c := New()
	c.ObserveRun(control.RunObservation{
		TaskRef: "github-review", Phase: "Succeeded", Provider: "k8s-job", Duration: 4 * time.Second,
		Timing: aei.Timing{ModelSeconds: 2, ModelCalls: -1, ToolCalls: -5, Measured: aei.StageModel},
	})
	f := gather(t, c)
	l := map[string]string{"task_ref": "github-review", "phase": "Succeeded", "provider": "k8s-job"}
	if got, _ := f.value("aei_runs_total", l); got != 1 {
		t.Errorf("runs = %v, want 1", got)
	}
	for _, name := range []string{"aei_run_model_calls_total", "aei_run_tool_calls_total"} {
		if got, ok := f.value(name, l); !ok || got != 0 {
			t.Errorf("%s = %v (present %v), want 0", name, got, ok)
		}
	}
	if got, _ := f.value("aei_run_duration_seconds", withStage(l, "model")); got != 2 {
		t.Errorf("model = %v, want 2", got)
	}
}

// A measured zero is a sample; an omitted stage is not. Two reports with
// provisioning 0s and 2s average 1s, and a third that omits provisioning does not
// change that.
func TestCollectorKeepsMeasuredZero(t *testing.T) {
	c := New()
	for _, tm := range []aei.Timing{
		{ProvisioningSeconds: 0, Measured: aei.StageProvisioning},
		{ProvisioningSeconds: 2, Measured: aei.StageProvisioning},
		{RuntimeSeconds: 5, Measured: aei.StageRuntime},
	} {
		c.ObserveRun(control.RunObservation{TaskRef: "t", Phase: "Succeeded", Provider: "p", Duration: time.Second, Timing: tm})
	}
	l := map[string]string{"task_ref": "t", "phase": "Succeeded", "provider": "p", "stage": "provisioning"}
	mf := gather(t, c)["aei_run_duration_seconds"]
	for _, m := range mf.GetMetric() {
		got := map[string]string{}
		for _, lp := range m.GetLabel() {
			got[lp.GetName()] = lp.GetValue()
		}
		if got["stage"] != l["stage"] {
			continue
		}
		if h := m.GetHistogram(); h.GetSampleCount() != 2 || h.GetSampleSum() != 2 {
			t.Errorf("provisioning count=%d sum=%v, want count 2 sum 2", h.GetSampleCount(), h.GetSampleSum())
		}
		return
	}
	t.Fatal("no provisioning series")
}

func withStage(l map[string]string, stage string) map[string]string {
	out := map[string]string{"stage": stage}
	for k, v := range l {
		out[k] = v
	}
	return out
}

// An empty taskRef or provider is labeled "unknown" rather than dropped, so the
// run still shows up.
func TestCollectorDefaultsLabels(t *testing.T) {
	c := New()
	c.ObserveRun(control.RunObservation{Phase: "Succeeded"})
	if body := scrape(t, c.Handler()); !strings.Contains(body, `aei_runs_total{phase="Succeeded",provider="unknown",task_ref="unknown"} 1`) {
		t.Errorf("empty taskRef and provider should label unknown\n%s", body)
	}
}

// End to end through the control plane: a tool-using run that reports through
// /complete shows its calls in /metrics under its provider, with the time spent
// waiting for the provider (provisioning) separate from the time in the model;
// a run the deadline ends shows the time it really ran.
func TestPlaneToMetrics(t *testing.T) {
	c := New()
	p := control.New(control.Config{Secret: []byte("k"), Observer: c})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	// A tool-using review that completes by callback.
	fa := &callbackProvider{dispatched: make(chan aei.Credential, 1)}
	runID, err := p.Submit(context.Background(), fa, aei.RunSpec{TaskRef: "github-review", Deadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cred := <-fa.dispatched
	complete(t, srv.URL, cred.Value, map[string]any{
		"run_id": runID, "result_ref": "res://1",
		"timing": map[string]any{
			"provisioning_seconds": 2.5, "runtime_seconds": 30, "model_seconds": 20,
			"model_calls": 6, "tool_calls": 9,
		},
	})

	// A review the deadline ends; the runtime never reports.
	const deadline = 200 * time.Millisecond
	fb := &callbackProvider{dispatched: make(chan aei.Credential, 1)}
	if _, err := p.Submit(context.Background(), fb, aei.RunSpec{TaskRef: "github-review-timeout", Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	<-fb.dispatched

	labels := func(task, phase string) map[string]string {
		return map[string]string{"task_ref": task, "phase": phase, "provider": "fake"}
	}
	stage := func(task, phase, s string) map[string]string {
		return withStage(labels(task, phase), s)
	}
	// ObserveRun writes its series one after another, so wait for the last one each
	// run writes that the assertions read, not just its run counter.
	mf := waitFor(t, c, func(mf families) bool {
		_, a := mf.value("aei_run_tool_calls_total", labels("github-review", "Succeeded"))
		_, b := mf.value("aei_run_duration_seconds", stage("github-review-timeout", "Failed", "total"))
		return a && b
	})

	for name, want := range map[string]float64{"aei_run_model_calls_total": 6, "aei_run_tool_calls_total": 9} {
		if got, _ := mf.value(name, labels("github-review", "Succeeded")); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if got, _ := mf.value("aei_run_duration_seconds", stage("github-review", "Succeeded", "provisioning")); got != 2.5 {
		t.Errorf("provisioning (waiting for the provider) = %v, want 2.5", got)
	}
	if got, _ := mf.value("aei_run_duration_seconds", stage("github-review", "Succeeded", "model")); got != 20 {
		t.Errorf("model = %v, want 20", got)
	}
	if got, ok := mf.value("aei_run_duration_seconds", stage("github-review-timeout", "Failed", "total")); !ok || got < deadline.Seconds() {
		t.Errorf("timed-out total = %v (present %v), want at least %v", got, ok, deadline.Seconds())
	}
	if _, ok := mf.value("aei_run_duration_seconds", stage("github-review-timeout", "Failed", "provisioning")); ok {
		t.Error("a timed-out run without a report should not record provisioning")
	}
}

// callbackProvider is an AsyncLauncher whose Await blocks until the deadline, so a
// run completes either by the runtime's /complete callback or at the deadline.
type callbackProvider struct{ dispatched chan aei.Credential }

func (f *callbackProvider) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, _ := f.Dispatch(ctx, spec, cred)
	r, _ := f.Await(ctx, h)
	return h, r.Err
}

func (f *callbackProvider) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	f.dispatched <- cred
	return aei.Handle{Provider: "fake", Ref: spec.ID}, nil
}

func (f *callbackProvider) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
}

func complete(t *testing.T, base, bearer string, body any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, base+"/aei/v1alpha1/complete", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("complete status = %d, want 202", resp.StatusCode)
	}
}

// families is one gather of the collector's registry, by metric name.
type families map[string]*dto.MetricFamily

// value returns the sample whose labels include labels: a counter's value, or a
// histogram's sum, which is the observed value when a single run was recorded.
func (f families) value(name string, labels map[string]string) (float64, bool) {
	mf, ok := f[name]
	if !ok {
		return 0, false
	}
	for _, m := range mf.GetMetric() {
		got := map[string]string{}
		for _, lp := range m.GetLabel() {
			got[lp.GetName()] = lp.GetValue()
		}
		match := true
		for k, want := range labels {
			if got[k] != want {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		if h := m.GetHistogram(); h != nil {
			return h.GetSampleSum(), true
		}
		return m.GetCounter().GetValue(), true
	}
	return 0, false
}

func gather(t *testing.T, c *Collector) families {
	t.Helper()
	mfs, err := c.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := families{}
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func waitFor(t *testing.T, c *Collector, ok func(families) bool) families {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		f := gather(t, c)
		if ok(f) {
			return f
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics never reached the expected state")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d", rec.Code)
	}
	return rec.Body.String()
}
