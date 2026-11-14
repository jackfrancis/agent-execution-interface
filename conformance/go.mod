module github.com/jackfrancis/agent-execution-interface/conformance

go 1.27.0

toolchain go1.27.4

require (
	github.com/jackfrancis/agent-execution-interface v0.0.0-00010101000000-000000000000
	github.com/jackfrancis/agent-execution-interface/aeiruntime v0.0.0-00010101000000-000000000000
)

// The conformance module is the one place both halves of the runtime wire meet
// in the same build, on purpose: it exists to verify the dispatch-side encoder
// (aei.Injection) and the runtime-side decoder (aeiruntime.Load) agree, while
// keeping either from depending on the other. Local replaces, no published
// versions; nothing depends on this module.
replace github.com/jackfrancis/agent-execution-interface => ../

replace github.com/jackfrancis/agent-execution-interface/aeiruntime => ../aeiruntime
