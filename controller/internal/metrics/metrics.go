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

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
)

// durationBuckets span sub-second to the typical run deadline (~15m), sized for
// agent runs (seconds to minutes) rather than the default web-latency buckets.
var durationBuckets = []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900}

// Collector implements control.Observer, recording each run's terminal outcome
// into Prometheus series labeled by task_ref and phase. It owns a private registry
// so the metrics are isolated and testable; Handler serves them at /metrics.
type Collector struct {
	reg      *prometheus.Registry
	runs     *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// Collector is the platform-supplied metrics sink for the control plane.
var _ control.Observer = (*Collector)(nil)

// New builds a Collector over a fresh registry that also carries the standard Go
// and process collectors, so the same /metrics endpoint reports runtime health.
func New() *Collector {
	reg := prometheus.NewRegistry()
	c := &Collector{
		reg: reg,
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aei_runs_total",
			Help: "AEI runs that reached a terminal phase, by task and phase.",
		}, []string{"task_ref", "phase"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "aei_run_duration_seconds",
			Help:    "AEI run timing in seconds, by task, phase, and stage (stage=total is provisioning+runtime).",
			Buckets: durationBuckets,
		}, []string{"task_ref", "phase", "stage"}),
	}
	reg.MustRegister(
		c.runs,
		c.duration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return c
}

// ObserveRun records one run's terminal outcome (control.Observer). It is safe for
// concurrent use. Timing sub-durations are reported as separate stages; stage
// "total" is the run's wall-clock (provisioning + runtime), the headline "how long
// did the run take" series.
func (c *Collector) ObserveRun(taskRef, phase string, t aei.Timing) {
	if taskRef == "" {
		taskRef = "unknown"
	}
	c.runs.WithLabelValues(taskRef, phase).Inc()
	c.duration.WithLabelValues(taskRef, phase, "total").Observe(t.ProvisioningSeconds + t.RuntimeSeconds)
	c.duration.WithLabelValues(taskRef, phase, "provisioning").Observe(t.ProvisioningSeconds)
	c.duration.WithLabelValues(taskRef, phase, "runtime").Observe(t.RuntimeSeconds)
	c.duration.WithLabelValues(taskRef, phase, "model").Observe(t.ModelSeconds)
}

// Handler serves the collected metrics in the Prometheus text exposition format.
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{Registry: c.reg})
}
