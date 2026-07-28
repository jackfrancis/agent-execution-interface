package conformance

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// requireShellRuntime skips when sh or curl is unavailable — the reference runtime
// needs both. The harness itself (Drive) needs neither; only this demonstration
// of it does.
func requireShellRuntime(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"sh", "curl"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("reference runtime needs %q on PATH: %v", bin, err)
		}
	}
}

// TestExternalRuntimeConforms drives a NON-Go reference runtime — POSIX sh + curl,
// testdata/runtime.sh — through the wire harness and asserts it conformed. This is
// the language-agnostic claim made executable: a program that only reads AEI_* and
// speaks the JSON routes, with no Go SDK and no shared symbol, satisfies the same
// contract the Go SDK does, on both credential paths.
func TestExternalRuntimeConforms(t *testing.T) {
	requireShellRuntime(t)
	cmd := RuntimeCmd{Argv: []string{"sh", "runtime.sh"}, Dir: "testdata"}

	for _, tc := range []struct {
		name string
		id   string
		cred aei.Credential
	}{
		{"push (injected credential)", "run-ext-push", aei.Credential{Value: "run-token-xyz", Kind: aei.Token}},
		{"pull (redeemed ticket)", "run-ext-pull", aei.Credential{Value: "tkt-once", Kind: aei.Ticket}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			spec := aei.RunSpec{
				ID:         tc.id,
				TaskRef:    "worklist://item/pr-4127",
				Parameters: map[string]string{"kind": "converse"},
				Deadline:   time.Minute,
			}
			obs, err := Drive(ctx, cmd, spec, tc.cred)
			if err != nil {
				t.Fatalf("Drive: %v", err)
			}
			if err := obs.Verify(spec, tc.cred); err != nil {
				t.Fatalf("non-Go runtime did not conform: %v\nstdout: %q\nstderr: %q", err, obs.Stdout, obs.Stderr)
			}
		})
	}
}
