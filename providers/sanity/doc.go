// Package sanity is the seed of aei-sanity: a portable battery that drives a
// candidate provider through the AEI contracts and asserts identical terminal
// outcomes, so "swap the provider" is a verifiable claim rather than a hope. This
// first smoke drives both reference providers — k8s-job (watched) and
// agent-sandbox (detached) — through the real control plane to an identical
// callback completion, proving the dispatch + injection + completion loop is
// provider-independent.
package sanity
