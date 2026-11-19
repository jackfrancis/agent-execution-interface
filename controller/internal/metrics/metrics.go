// Package metrics is the AEI controller's Prometheus adapter: it implements the
// control.Observer seam and exposes the recorded run metrics at /metrics. It lives
// in the controller module (not control/) so the lean control plane keeps no
// Prometheus dependency — the platform brings the metrics backend, exactly as it
// brings the provider and the identity authority (docs/adr, the seam pattern).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackfrancis/agent-execution-interface/control"
)

// durationBuckets span sub-second to the typical run deadline (~15m), sized for
// agent runs (seconds to minutes) rather than the default web-latency buckets.
var durationBuckets = []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900}

// Collector implements control.Observer, recording each run's terminal outcome
// into Prometheus series labeled by task_ref, phase, and provider. It owns a
// private registry so the metrics are isolated and testable; Handler serves them
// at /metrics.
type Collector struct {
	reg        *prometheus.Registry
	runs       *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	modelCalls *prometheus.CounterVec
	toolCalls  *prometheus.CounterVec
}

// Collector is the platform-supplied metrics sink for the control plane.
var _ control.Observer = (*Collector)(nil)

// runLabels label every run series, so an operator can join them.
var runLabels = []string{"task_ref", "phase", "provider"}

// New builds a Collector over a fresh registry that also carries the standard Go
// and process collectors, so the same /metrics endpoint reports runtime health.
func New() *Collector {
	reg := prometheus.NewRegistry()
	c := &Collector{
		reg: reg,
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aei_runs_total",
			Help: "AEI runs that reached a terminal phase, by task, phase, and provider.",
		}, runLabels),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "aei_run_duration_seconds",
			Help: "AEI run timing in seconds, by task, phase, provider, and stage. " +
				"stage=total is the controller-measured wall-clock from dispatch to the terminal signal, recorded for every run; " +
				"provisioning (waiting for the provider to start the runtime), runtime, and model are the runtime's report, each recorded only when reported.",
			Buckets: durationBuckets,
		}, append(append([]string(nil), runLabels...), "stage")),
		modelCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aei_run_model_calls_total",
			Help: "Model calls the runtime reported across AEI runs, by task, phase, and provider.",
		}, runLabels),
		toolCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aei_run_tool_calls_total",
			Help: "Tool calls the runtime reported across AEI runs, by task, phase, and provider.",
		}, runLabels),
	}
	reg.MustRegister(
		c.runs,
		c.duration,
		c.modelCalls,
		c.toolCalls,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return c
}

// ObserveRun records one run's terminal outcome (control.Observer). It is safe for
// concurrent use. Stage "total" is the controller-measured wall-clock, the
// headline "how long did the run take" series; it is recorded for every observed
// run, including one ended by the watch or the deadline. Every field of the
// runtime's timing report is optional (docs/runtime-abi.md §3), so each reported
// stage is recorded only when its value is positive: a run that did not report a
// stage does not drag it toward zero. The call counters are created for every run
// and grow only by a positive count; a negative count (a malformed report) is
// ignored rather than passed to Counter.Add, which would panic.
func (c *Collector) ObserveRun(o control.RunObservation) {
	taskRef, provider := o.TaskRef, o.Provider
	if taskRef == "" {
		taskRef = "unknown"
	}
	if provider == "" {
		provider = "unknown"
	}
	c.runs.WithLabelValues(taskRef, o.Phase, provider).Inc()
	stage := func(name string, seconds float64) {
		if seconds > 0 {
			c.duration.WithLabelValues(taskRef, o.Phase, provider, name).Observe(seconds)
		}
	}
	stage("total", o.Duration.Seconds())
	stage("provisioning", o.Timing.ProvisioningSeconds)
	stage("runtime", o.Timing.RuntimeSeconds)
	stage("model", o.Timing.ModelSeconds)

	count := func(v *prometheus.CounterVec, n int) {
		ctr := v.WithLabelValues(taskRef, o.Phase, provider)
		if n > 0 {
			ctr.Add(float64(n))
		}
	}
	count(c.modelCalls, o.Timing.ModelCalls)
	count(c.toolCalls, o.Timing.ToolCalls)
}

// Handler serves the collected metrics in the Prometheus text exposition format.
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{Registry: c.reg})
}
