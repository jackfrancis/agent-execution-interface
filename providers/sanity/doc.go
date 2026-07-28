// Package sanity is the seed of aei-sanity: a portable battery that drives a
// candidate provider through the AEI contracts and asserts identical terminal
// outcomes, so "swap the provider" is a verifiable claim rather than a hope. It
// drives all three reference providers through the real control plane to the same
// Succeeded outcome, across both credential-delivery axes: the push/workload shape —
// k8s-job and agent-sandbox, which read a pushed token out of the workload the
// provider created — and the durable pull/A2A shape — agent-substrate, whose
// credential is a single-use ticket delivered over A2A message metadata and redeemed
// for a token before completion. Holding those different shapes to one terminal
// outcome is what makes the dispatch + injection + completion loop
// provider-independent.
//
// The suite also covers the three ways a run reaches a terminal state, which the
// control plane races (docs/design.md, Completion): the runtime's /complete callback
// (every provider above), the provider's watch backstop — asserted for EVERY
// pod-shaped provider, since a workload that dies before its runtime starts can
// never call back and only the substrate's own object carries the cause — and the
// deadline backstop (a dispatched run that reaches no terminal condition at all,
// finalized at its budget). Together these are the
// dispatch · callback · watch-backstop · deadline · ticket-redemption · injection
// round-trip dimensions Phase 3 names.
package sanity
