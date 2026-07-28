package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// The collector records terminal runs and exposes them at /metrics, labeled by
// task and phase, with a per-stage duration breakdown.
func TestCollectorRecordsAndExposes(t *testing.T) {
	c := New()
	c.ObserveRun("github-converse", "Succeeded", aei.Timing{ProvisioningSeconds: 2, RuntimeSeconds: 8, ModelSeconds: 5})
	c.ObserveRun("github-converse", "Failed", aei.Timing{ProvisioningSeconds: 1, RuntimeSeconds: 1})

	body := scrape(t, c.Handler())
	for _, want := range []string{
		`aei_runs_total{phase="Succeeded",task_ref="github-converse"} 1`,
		`aei_runs_total{phase="Failed",task_ref="github-converse"} 1`,
		`aei_run_duration_seconds_bucket`,
		`stage="total"`,
		`stage="provisioning"`,
		`stage="model"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
}

// An empty taskRef is labeled "unknown" rather than dropped, so a run with no task
// still shows up.
func TestCollectorDefaultsTaskRef(t *testing.T) {
	c := New()
	c.ObserveRun("", "Succeeded", aei.Timing{})
	if body := scrape(t, c.Handler()); !strings.Contains(body, `task_ref="unknown"`) {
		t.Errorf("empty taskRef should label unknown\n%s", body)
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
