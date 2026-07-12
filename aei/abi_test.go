package aei_test

import (
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// TestInjectionEncodesPushRun checks the push path: the token rides
// AEI_RUN_CREDENTIAL, params are JSON, the deadline is absolute millis, and an
// unset broker URL is omitted.
func TestInjectionEncodesPushRun(t *testing.T) {
	dispatched := time.UnixMilli(1789000000000)
	spec := aei.RunSpec{
		ID:         "run-abc",
		TaskRef:    "worklist://item/pr-4127",
		Parameters: map[string]string{"kind": "converse"},
		Deadline:   5 * time.Minute,
		Identity:   aei.IdentityRequest{Audience: "team-a-agent"},
	}

	m := aei.Injection(spec, aei.Credential{Value: "tok", Kind: aei.Token}, "http://cp/aei/v1alpha1", "", dispatched)

	want := map[string]string{
		aei.EnvRunID:        "run-abc",
		aei.EnvTaskRef:      "worklist://item/pr-4127",
		aei.EnvParams:       `{"kind":"converse"}`,
		aei.EnvControlURL:   "http://cp/aei/v1alpha1",
		aei.EnvCredential:   "tok",
		aei.EnvDispatchedAt: "1789000000000",
		aei.EnvDeadline:     "1789000300000", // dispatched + 5m
		aei.EnvAudience:     "team-a-agent",
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %q, want %q", k, m[k], v)
		}
	}
	if _, ok := m[aei.EnvTicket]; ok {
		t.Error("push path must not set AEI_RUN_TICKET")
	}
	if _, ok := m[aei.EnvBrokerURL]; ok {
		t.Error("broker URL must be omitted when unset")
	}
}

// TestInjectionPullUsesTicketAndBrokerSplit checks the pull path and the broker
// split: the ticket rides AEI_RUN_TICKET, the broker URL is emitted when it
// differs, and with no dispatchedAt there is no timing pair.
func TestInjectionPullUsesTicketAndBrokerSplit(t *testing.T) {
	m := aei.Injection(
		aei.RunSpec{ID: "r", Deadline: time.Minute},
		aei.Credential{Value: "tkt", Kind: aei.Ticket},
		"http://cp", "http://broker", time.Time{},
	)

	if m[aei.EnvTicket] != "tkt" {
		t.Errorf("AEI_RUN_TICKET = %q, want tkt", m[aei.EnvTicket])
	}
	if _, ok := m[aei.EnvCredential]; ok {
		t.Error("pull path must not set AEI_RUN_CREDENTIAL")
	}
	if m[aei.EnvBrokerURL] != "http://broker" {
		t.Errorf("AEI_BROKER_URL = %q, want http://broker", m[aei.EnvBrokerURL])
	}
	if _, ok := m[aei.EnvDispatchedAt]; ok {
		t.Error("no dispatchedAt ⇒ no AEI_DISPATCHED_AT")
	}
	if _, ok := m[aei.EnvDeadline]; ok {
		t.Error("no dispatchedAt ⇒ no absolute AEI_DEADLINE")
	}
}
