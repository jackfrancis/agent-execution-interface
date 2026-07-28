package aei

import (
	"encoding/json"
	"strconv"
	"time"
)

// The reserved AEI_* injection keys the dispatch side fills and a runtime reads
// (docs/runtime-abi.md §1.1). The runtime SDK (module .../aeiruntime) is separate
// and deliberately keeps its own copy of these names: the two halves meet at the
// wire, so both sides cite the spec rather than sharing a Go symbol across a
// module boundary. Keep these in sync with docs/runtime-abi.md.
const (
	EnvRunID        = "AEI_RUN_ID"         // unique run id; completion-correlation key
	EnvTaskRef      = "AEI_TASK_REF"       // opaque handle to the work
	EnvParams       = "AEI_PARAMS"         // opaque per-run params, a JSON object string
	EnvDispatchedAt = "AEI_DISPATCHED_AT"  // dispatch-start, unix millis
	EnvCredential   = "AEI_RUN_CREDENTIAL" // push path: the run credential (bearer)
	EnvTicket       = "AEI_RUN_TICKET"     // pull path: single-use redemption ticket
	EnvDeadline     = "AEI_DEADLINE"       // run wall-clock budget end, unix millis
	EnvControlURL   = "AEI_CONTROL_URL"    // base URL for /complete
	EnvBrokerURL    = "AEI_BROKER_URL"     // base URL for /redeem + /vend; absent ⇒ control
	EnvAudience     = "AEI_AUDIENCE"       // audience the run credential is bound to
	EnvSession      = "AEI_SESSION"        // durable session/conversation key (optional; enables warm session affinity)
)

// Injection builds the substrate-neutral AEI_* map a provider delivers to a run
// (docs/runtime-abi.md §1). A workload provider wraps it as container env; a
// durable provider forwards it as A2A message metadata. It is the single encode
// site, so every provider injects identically — the cross-provider conformance
// invariant (aei-sanity). It is the exact inverse of the runtime SDK's decoder.
//
// dispatchedAt stamps the dispatch-start for launcher-symmetric provisioning
// timing; the absolute deadline is derived from it. Credential delivery follows
// cred.Kind: a Ticket rides AEI_RUN_TICKET (pull), anything else AEI_RUN_CREDENTIAL
// (push). The broker URL is emitted only when it differs from the control URL
// (B-with-default), and empty optional values are omitted so they cannot shadow a
// durable runtime's environment.
func Injection(spec RunSpec, cred Credential, controlURL, brokerURL string, dispatchedAt time.Time) map[string]string {
	m := map[string]string{
		EnvRunID:      spec.ID,
		EnvControlURL: controlURL,
	}
	if spec.TaskRef != "" {
		m[EnvTaskRef] = spec.TaskRef
	}
	if len(spec.Parameters) > 0 {
		if b, err := json.Marshal(spec.Parameters); err == nil {
			m[EnvParams] = string(b)
		}
	}
	if !dispatchedAt.IsZero() {
		m[EnvDispatchedAt] = strconv.FormatInt(dispatchedAt.UnixMilli(), 10)
		if spec.Deadline > 0 {
			m[EnvDeadline] = strconv.FormatInt(dispatchedAt.Add(spec.Deadline).UnixMilli(), 10)
		}
	}
	if brokerURL != "" && brokerURL != controlURL {
		m[EnvBrokerURL] = brokerURL
	}
	if spec.Identity.Audience != "" {
		m[EnvAudience] = spec.Identity.Audience
	}
	if spec.Session != "" {
		m[EnvSession] = spec.Session
	}
	switch cred.Kind {
	case Ticket:
		if cred.Value != "" {
			m[EnvTicket] = cred.Value
		}
	default:
		if cred.Value != "" {
			m[EnvCredential] = cred.Value
		}
	}
	return m
}
