// Package authority tests authority.sh (what `make authority` runs) against a fake
// kubectl, covering the races and failures a live cluster rarely shows on demand.
package authority

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeKubectl answers each kind of call from a queue file in its state dir: one
// "exit|stdout|stderr" line per call, the last line repeating. It logs every call
// and saves the stdin of patch/create calls.
const fakeKubectl = `#!/usr/bin/env bash
d=$FAKE_DIR
args="$*"
echo "$args" >> "$d/calls"
case "$args" in
  *"get secret"*) op=get-secret;;
  *"patch secret"*) op=patch-secret;;
  *"create secret"*) op=create-secret;;
  *"get deploy/"*) op=get-deploy;;
  *"rollout restart"*) op=restart;;
  *"rollout status"*) op=status;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99;;
esac
case $op in patch-secret|create-secret) cat > "$d/$op.stdin";; esac
f="$d/$op"
[ -f "$f" ] || { echo "fake kubectl: no response queued for $op" >&2; exit 98; }
line=$(head -n 1 "$f")
if [ "$(wc -l < "$f")" -gt 1 ]; then tail -n +2 "$f" > "$f.next" && mv "$f.next" "$f"; fi
IFS='|' read -r code out err <<< "$line"
[ -n "$out" ] && printf '%s' "$out"
[ -n "$err" ] && echo "$err" >&2
exit "$code"
`

const fakeGenkey = `#!/usr/bin/env bash
echo AEI_ED25519_PRIVATE_KEY=PRIVKEY
echo AEI_ED25519_PUBLIC_KEY=PUBKEY
`

const (
	notFound      = `1||Error from server (NotFound): secrets "aei-signing" not found`
	deployMissing = `1||Error from server (NotFound): deployments.apps "aei-controller" not found`
	unreachable   = `1||Unable to connect to the server: dial tcp 127.0.0.1:6443: connect: connection refused`
	conflict      = `1||Error from server (Conflict): Operation cannot be fulfilled on secrets "aei-signing": the object has been modified; please apply your changes to the latest version and try again`
	alreadyExists = `1||Error from server (AlreadyExists): secrets "aei-signing" already exists`
	noKeyAtRV5    = `0|5 |`
	hasKey        = `0|9 c29tZWtleQ==|`
	ok            = `0||`
)

type result struct {
	out, calls, patchBody, createBody string
	err                               error
}

func run(t *testing.T, queues map[string][]string) result {
	t.Helper()
	dir := t.TempDir()
	write := func(name, body string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	kubectl := write("kubectl", fakeKubectl, 0o755)
	genkey := write("genkey", fakeGenkey, 0o755)
	for op, lines := range queues {
		write(op, strings.Join(lines, "\n")+"\n", 0o644)
	}
	cmd := exec.Command("bash", "authority.sh")
	cmd.Env = append(os.Environ(),
		"FAKE_DIR="+dir,
		"KUBECTL="+kubectl+" --context test",
		"GENKEY="+genkey,
	)
	out, err := cmd.CombinedOutput()
	read := func(name string) string { b, _ := os.ReadFile(filepath.Join(dir, name)); return string(b) }
	return result{out: string(out), calls: read("calls"), patchBody: read("patch-secret.stdin"), createBody: read("create-secret.stdin"), err: err}
}

func mustSucceed(t *testing.T, r result) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("authority.sh failed: %v\n%s\ncalls:\n%s", r.err, r.out, r.calls)
	}
}

func mustFail(t *testing.T, r result) {
	t.Helper()
	if r.err == nil {
		t.Fatalf("authority.sh succeeded; want failure\n%s\ncalls:\n%s", r.out, r.calls)
	}
}

// Fresh install: no secret, no controller yet. It creates the secret, never via
// argv, and reports the key without restarting anything.
func TestFreshInstallCreatesSecret(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {notFound}, "create-secret": {ok}, "get-deploy": {deployMissing}})
	mustSucceed(t, r)
	if !strings.Contains(r.createBody, "AEI_ED25519_PRIVATE_KEY=PRIVKEY") {
		t.Errorf("create stdin = %q; want the keypair", r.createBody)
	}
	if strings.Contains(r.calls, "PRIVKEY") {
		t.Error("the private key appeared on a kubectl command line")
	}
	if strings.Contains(r.calls, "rollout") {
		t.Error("restarted a controller that does not exist")
	}
	if !strings.Contains(r.out, "PUBKEY") {
		t.Errorf("output does not show the public key:\n%s", r.out)
	}
}

// Redeploy: a key exists, so it is reused and nothing is written.
func TestExistingKeyIsReused(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {hasKey}})
	mustSucceed(t, r)
	if strings.Contains(r.calls, "patch") || strings.Contains(r.calls, "create") || strings.Contains(r.calls, "rollout") {
		t.Errorf("wrote or restarted despite an existing key; calls:\n%s", r.calls)
	}
	if !strings.Contains(r.out, "reusing") {
		t.Errorf("output = %q; want it to say the key is reused", r.out)
	}
}

// A read failure that is not NotFound must not be taken for a missing key.
func TestReadErrorIsNotTreatedAsMissing(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {unreachable}})
	mustFail(t, r)
	if strings.Contains(r.calls, "patch") || strings.Contains(r.calls, "create") {
		t.Errorf("wrote after a failed read; calls:\n%s", r.calls)
	}
	if !strings.Contains(r.out, "not treating it as missing") {
		t.Errorf("output does not explain the refusal:\n%s", r.out)
	}
}

// A secret without a key is patched conditionally on the resourceVersion it was
// read at; when another deploy writes a key first (Conflict), the re-read reuses it.
func TestConcurrentWriterWinsOnConflict(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {noKeyAtRV5, hasKey}, "patch-secret": {conflict}})
	mustSucceed(t, r)
	if !strings.Contains(r.patchBody, `"resourceVersion":"5"`) {
		t.Errorf("patch = %q; want it conditioned on resourceVersion 5", r.patchBody)
	}
	if n := strings.Count(r.calls, "patch secret"); n != 1 {
		t.Errorf("patched %d times; want 1, then reuse the winner's key", n)
	}
	if !strings.Contains(r.out, "reusing") {
		t.Errorf("did not reuse the concurrent writer's key:\n%s", r.out)
	}
}

// Two installs racing to create: the loser sees AlreadyExists, re-reads, reuses.
func TestConcurrentCreateIsReused(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {notFound, hasKey}, "create-secret": {alreadyExists}})
	mustSucceed(t, r)
	if !strings.Contains(r.out, "reusing") {
		t.Errorf("did not reuse the winner's key:\n%s", r.out)
	}
}

// Any other patch failure stops instead of retrying blindly.
func TestPatchFailureStops(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {noKeyAtRV5}, "patch-secret": {`1||Error from server (Forbidden): secrets "aei-signing" is forbidden`}})
	mustFail(t, r)
}

// It gives up rather than loop forever when the secret keeps changing.
func TestGivesUpAfterRepeatedConflicts(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {noKeyAtRV5}, "patch-secret": {conflict}})
	mustFail(t, r)
	if !strings.Contains(r.out, "gave up") {
		t.Errorf("output = %q; want a give-up message", r.out)
	}
}

// A new key for a running controller: restart, wait, and only then call it active.
func TestNewKeyRollsRunningController(t *testing.T) {
	r := run(t, map[string][]string{"get-secret": {noKeyAtRV5}, "patch-secret": {ok}, "get-deploy": {ok}, "restart": {ok}, "status": {ok}})
	mustSucceed(t, r)
	restart, status := strings.Index(r.calls, "rollout restart"), strings.Index(r.calls, "rollout status")
	if restart < 0 || status < restart {
		t.Fatalf("want restart then rollout status; calls:\n%s", r.calls)
	}
	if !strings.Contains(r.out, "provisioned and active") {
		t.Errorf("output = %q; want the key reported active", r.out)
	}
}

// A failed restart or rollout fails the target and never reports the key active.
func TestFailedRolloutIsNotReportedActive(t *testing.T) {
	for name, q := range map[string]map[string][]string{
		"restart fails":     {"get-secret": {noKeyAtRV5}, "patch-secret": {ok}, "get-deploy": {ok}, "restart": {`1||error: failed to patch`}},
		"rollout fails":     {"get-secret": {noKeyAtRV5}, "patch-secret": {ok}, "get-deploy": {ok}, "restart": {ok}, "status": {`1||error: timed out waiting for the condition`}},
		"deploy unreadable": {"get-secret": {notFound}, "create-secret": {ok}, "get-deploy": {unreachable}},
	} {
		t.Run(name, func(t *testing.T) {
			r := run(t, q)
			mustFail(t, r)
			if strings.Contains(r.out, "PUBKEY") || strings.Contains(r.out, "and active") {
				t.Errorf("advertised the key despite the failure:\n%s", r.out)
			}
			if !strings.Contains(r.out, "NOT active") && name != "deploy unreadable" {
				t.Errorf("output does not say the key is not active:\n%s", r.out)
			}
		})
	}
}
