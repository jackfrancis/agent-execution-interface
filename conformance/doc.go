// Package conformance verifies the AEI runtime wire (docs/runtime-abi.md) by
// round-tripping the dispatch-side encoder through the runtime-side decoder.
//
// The two sides live in separate modules and deliberately share no Go symbol —
// aei.Injection (encode) and aeiruntime.Load (decode) each keep their own copy of
// the AEI_* keys, "meeting at the wire" (see aei/abi.go and aeiruntime). That
// discipline is what keeps a runtime author's module free of provider code, but
// it also means nothing at compile time stops the two halves drifting. This
// package is that missing check: it is the executable form of runtime-abi.md §1
// and §2, the safety net the deliberate decoupling requires.
//
// Two levels of check live here. The Go round-trip (wire_test.go) feeds
// aei.Injection through the aeiruntime SDK decoder — encode/decode agreement. The
// external harness (harness.go, Drive) goes further: it drives an arbitrary
// executable — a shell script, a Python program, any language — through the wire
// as an opaque process, injecting AEI_* as environment and observing its /redeem
// and /complete calls, so "any-language runtime" is demonstrated, not merely
// asserted (external_test.go drives a POSIX sh + curl reference runtime that
// carries no SDK at all).
//
// It complements — it does not duplicate — providers/sanity: sanity asserts a
// *provider* drives a run to completion the same way as every other provider
// (dispatch behaviour), while this asserts the *wire* itself round-trips
// (encode/decode agreement), across both transports and both credential paths.
package conformance
