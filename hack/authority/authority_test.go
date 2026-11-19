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
// "exit^stdout^stderr" line per call, the last line repeating. It logs every call
// and saves the stdin of patch/create calls.
const fakeKubectl = `#!/usr/bin/env bash
d=$FAKE_DIR
args="$*"
echo "$args" >> "$d/calls"
case "$args" in
  *"get secret"*PUBLIC_KEY*) op=get-pub;;
  *"get secret"*) op=get-secret;;
  *"patch secret"*) op=patch-secret;;
  *"create secret"*) op=create-secret;;
  *"get deploy/"*) op=get-deploy;;
  *"patch deploy/"*) op=patch-deploy;;
  *"rollout status"*) op=status;;
  *) echo "fake kubectl: unexpected call: $args" >&2; exit 99;;
esac
case $op in patch-secret|create-secret|patch-deploy) cat > "$d/$op.stdin";; esac
f="$d/$op"
[ -f "$f" ] || { echo "fake kubectl: no response queued for $op" >&2; exit 98; }
line=$(head -n 1 "$f")
if [ "$(wc -l < "$f")" -gt 1 ]; then tail -n +2 "$f" > "$f.next" && mv "$f.next" "$f"; fi
IFS='^' read -r code out err <<< "$line"
[ -n "$out" ] && printf '%s' "$out"
[ -n "$err" ] && echo "$err" >&2
exit "$code"
`

const fakeGenkey = `#!/usr/bin/env bash
echo AEI_ED25519_PRIVATE_KEY=PRIVKEY
echo AEI_ED25519_PUBLIC_KEY=PUBKEY
`

// fakeDerive stands in for "genkey -public-from-private": PRIVKEY's public key is
// PUBKEY, and anything else is an inconsistent key.
const fakeDerive = `#!/usr/bin/env bash
in=$(cat)
[ "$in" = PRIVKEY ] || { echo "genkey: private key is inconsistent" >&2; exit 1; }
echo PUBKEY
`

const (
	notFound      = `1^^Error from server (NotFound): secrets "aei-signing" not found`
	deployMissing = `1^^Error from server (NotFound): deployments.apps "aei-controller" not found`
	unreachable   = `1^^Unable to connect to the server: dial tcp 127.0.0.1:6443: connect: connection refused`
	conflict      = `1^^Error from server (Conflict): Operation cannot be fulfilled on secrets "aei-signing": the object has been modified; please apply your changes to the latest version and try again`
	// What kubectl actually prints for an API AlreadyExists on create secret: its
	// client message, without "(AlreadyExists)".
	alreadyExists = `1^^error: failed to create secret secrets "aei-signing" already exists`
	noKeyAtRV5    = `0^5 ^`
	hasKey        = `0^9 UFJJVktFWQ==^`
	// activate's read: resourceVersion|private|public, base64 as the API returns
	// them. base64("PRIVKEY") = UFJJVktFWQ==, base64("PUBKEY") = UFVCS0VZ.
	pubKey   = `0^9|UFJJVktFWQ==|UFVCS0VZ^`
	privOnly = `0^7|UFJJVktFWQ==|^`         // the old Deployment's documented setup
	stalePub = `0^7|UFJJVktFWQ==|T0xES0VZ^` // public field = "OLDKEY"
	badPriv  = `0^7|QkFES0VZ|^`             // base64("BADKEY"): unusable
	noPriv   = `0^7||UFVCS0VZ^`
	onKey    = `0^PUBKEY^` // the pod template already carries this key
	notOnKey = `0^^`       // no (or another) key on the pod template
	ok       = `0^^`
)

type result struct {
	out, calls, patchBody, createBody, deployPatch string
	err                                            error
}

func run(t *testing.T, mode string, queues map[string][]string) result {
	t.Helper()
	dir := t.TempDir()
	write := func(name, body string, perm os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), perm); err != nil {
			t.Fatal(err)
		}
		return p
	}
	kubectl := write("kubectl", fakeKubectl, 0o755)
	genkey := write("genkey", fakeGenkey, 0o755)
	derive := write("derive", fakeDerive, 0o755)
	for op, lines := range queues {
		write(op, strings.Join(lines, "\n")+"\n", 0o644)
	}
	args := []string{"authority.sh"}
	if mode != "" {
		args = append(args, mode)
	}
	cmd := exec.Command("bash", args...)
	cmd.Env = append(os.Environ(),
		"FAKE_DIR="+dir,
		"KUBECTL="+kubectl+" --context test",
		"GENKEY="+genkey,
		"DERIVE="+derive,
	)
	out, err := cmd.CombinedOutput()
	read := func(name string) string { b, _ := os.ReadFile(filepath.Join(dir, name)); return string(b) }
	return result{
		out: string(out), calls: read("calls"), err: err,
		patchBody: read("patch-secret.stdin"), createBody: read("create-secret.stdin"), deployPatch: read("patch-deploy.stdin"),
	}
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
// argv, and has nothing to roll.
func TestFreshInstallCreatesSecret(t *testing.T) {
	r := run(t, "", map[string][]string{"get-secret": {notFound}, "create-secret": {ok}, "get-pub": {pubKey}, "get-deploy": {deployMissing}})
	mustSucceed(t, r)
	if !strings.Contains(r.createBody, "AEI_ED25519_PRIVATE_KEY=PRIVKEY") {
		t.Errorf("create stdin = %q; want the keypair", r.createBody)
	}
	if strings.Contains(r.calls, "PRIVKEY") {
		t.Error("the private key appeared on a kubectl command line")
	}
	if strings.Contains(r.calls, "patch deploy") || strings.Contains(r.calls, "rollout") {
		t.Error("rolled a controller that does not exist")
	}
	if !strings.Contains(r.out, "PUBKEY") {
		t.Errorf("output does not show the public key:\n%s", r.out)
	}
}

// Redeploy with the controller already on the key: nothing is written or rolled,
// and the key is reported active only after the rollout status check.
func TestExistingKeyAlreadyActive(t *testing.T) {
	r := run(t, "", map[string][]string{"get-secret": {hasKey}, "get-pub": {pubKey}, "get-deploy": {onKey}, "status": {ok}})
	mustSucceed(t, r)
	for _, w := range []string{"patch", "create"} {
		if strings.Contains(r.calls, w) {
			t.Errorf("called %s despite an existing, active key; calls:\n%s", w, r.calls)
		}
	}
	if !strings.Contains(r.calls, "rollout status") || !strings.Contains(r.out, "reusing") || !strings.Contains(r.out, "active") {
		t.Errorf("want reuse, a rollout check, then active:\n%s\ncalls:\n%s", r.out, r.calls)
	}
}

// A previous run saved the key but never rolled the controller onto it (its
// restart failed). The retry must finish activation, not report success.
func TestRetryFinishesInterruptedActivation(t *testing.T) {
	r := run(t, "", map[string][]string{"get-secret": {hasKey}, "get-pub": {pubKey}, "get-deploy": {notOnKey}, "patch-deploy": {ok}, "status": {ok}})
	mustSucceed(t, r)
	if !strings.Contains(r.deployPatch, `"agents.x-k8s.io/signing-public-key":"PUBKEY"`) {
		t.Errorf("deploy patch = %q; want the key stamped on the pod template", r.deployPatch)
	}
	patch, status := strings.Index(r.calls, "patch deploy"), strings.Index(r.calls, "rollout status")
	if patch < 0 || status < patch {
		t.Fatalf("want the template patched, then the rollout awaited; calls:\n%s", r.calls)
	}
}

// A read failure that is not NotFound must not be taken for a missing key.
func TestReadErrorIsNotTreatedAsMissing(t *testing.T) {
	r := run(t, "", map[string][]string{"get-secret": {unreachable}})
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
	r := run(t, "provision", map[string][]string{"get-secret": {noKeyAtRV5, hasKey}, "patch-secret": {conflict}})
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

// Two installs racing to create: the loser gets kubectl's "already exists",
// re-reads, and reuses the winner's key.
func TestConcurrentCreateIsReused(t *testing.T) {
	r := run(t, "provision", map[string][]string{"get-secret": {notFound, hasKey}, "create-secret": {alreadyExists}})
	mustSucceed(t, r)
	if n := strings.Count(r.calls, "create secret"); n != 1 {
		t.Errorf("created %d times; want 1", n)
	}
	if !strings.Contains(r.out, "reusing") {
		t.Errorf("did not reuse the winner's key:\n%s", r.out)
	}
}

// Any other write failure stops instead of retrying blindly.
func TestWriteFailureStops(t *testing.T) {
	for name, q := range map[string]map[string][]string{
		"patch":  {"get-secret": {noKeyAtRV5}, "patch-secret": {`1||Error from server (Forbidden): secrets "aei-signing" is forbidden`}},
		"create": {"get-secret": {notFound}, "create-secret": {`1||error: failed to create secret secrets is forbidden`}},
	} {
		t.Run(name, func(t *testing.T) { mustFail(t, run(t, "provision", q)) })
	}
}

// It gives up rather than loop forever when the secret keeps changing.
func TestGivesUpAfterRepeatedConflicts(t *testing.T) {
	r := run(t, "provision", map[string][]string{"get-secret": {noKeyAtRV5}, "patch-secret": {conflict}})
	mustFail(t, r)
	if !strings.Contains(r.out, "gave up") {
		t.Errorf("output = %q; want a give-up message", r.out)
	}
}

// provision alone (what deploy and dev-up run before applying the controller)
// never touches the Deployment: the old one may not be able to start on the key.
func TestProvisionLeavesDeploymentAlone(t *testing.T) {
	r := run(t, "provision", map[string][]string{"get-secret": {notFound}, "create-secret": {ok}})
	mustSucceed(t, r)
	if strings.Contains(r.calls, "deploy") || strings.Contains(r.calls, "rollout") {
		t.Errorf("provision touched the controller; calls:\n%s", r.calls)
	}
}

// A failed roll, rollout or Deployment read fails the step and never reports the
// key active.
func TestFailedActivationIsNotReportedActive(t *testing.T) {
	for name, q := range map[string]map[string][]string{
		"stale public key":  {"get-pub": {stalePub}},
		"unusable private":  {"get-pub": {badPriv}},
		"no private key":    {"get-pub": {noPriv}},
		"patch fails":       {"get-pub": {pubKey}, "get-deploy": {notOnKey}, "patch-deploy": {`1||error: deployments.apps "aei-controller" is forbidden`}},
		"rollout fails":     {"get-pub": {pubKey}, "get-deploy": {notOnKey}, "patch-deploy": {ok}, "status": {`1||error: timed out waiting for the condition`}},
		"rollout stuck":     {"get-pub": {pubKey}, "get-deploy": {onKey}, "status": {`1||error: timed out waiting for the condition`}},
		"deploy unreadable": {"get-pub": {pubKey}, "get-deploy": {unreachable}},
	} {
		t.Run(name, func(t *testing.T) {
			r := run(t, "activate", q)
			mustFail(t, r)
			if strings.Contains(r.out, "key active") || strings.Contains(r.out, "starts with this key") {
				t.Errorf("reported the key active despite the failure:\n%s", r.out)
			}
		})
	}
}

// An install whose secret holds only AEI_ED25519_PRIVATE_KEY (what the previous
// Deployment documented) activates: the public key is derived, backfilled with a
// patch conditioned on the resourceVersion read, and announced.
func TestPrivateOnlySecretIsBackfilled(t *testing.T) {
	r := run(t, "", map[string][]string{"get-secret": {hasKey}, "get-pub": {privOnly}, "patch-secret": {ok}, "get-deploy": {notOnKey}, "patch-deploy": {ok}, "status": {ok}})
	mustSucceed(t, r)
	if !strings.Contains(r.patchBody, `"resourceVersion":"7"`) || !strings.Contains(r.patchBody, `"AEI_ED25519_PUBLIC_KEY":"PUBKEY"`) {
		t.Errorf("backfill patch = %q; want PUBKEY conditioned on resourceVersion 7", r.patchBody)
	}
	if strings.Contains(r.patchBody, "PRIVATE") {
		t.Errorf("backfill patch rewrote the private key: %q", r.patchBody)
	}
	if !strings.Contains(r.deployPatch, "PUBKEY") || !strings.Contains(r.out, "signing key active") {
		t.Errorf("want the derived key stamped and reported active:\n%s", r.out)
	}
}

// A backfill that loses to a concurrent writer re-reads and uses what is there.
func TestBackfillConflictRereads(t *testing.T) {
	r := run(t, "activate", map[string][]string{"get-pub": {privOnly, pubKey}, "patch-secret": {conflict}, "get-deploy": {onKey}, "status": {ok}})
	mustSucceed(t, r)
	if n := strings.Count(r.calls, "patch secret"); n != 1 {
		t.Errorf("patched %d times; want 1", n)
	}
}

// A stale public field is never announced, and nothing is rolled onto it.
func TestStalePublicKeyIsRejected(t *testing.T) {
	r := run(t, "", map[string][]string{"get-secret": {hasKey}, "get-pub": {stalePub}})
	mustFail(t, r)
	if strings.Contains(r.out, "OLDKEY") || strings.Contains(r.calls, "patch") || strings.Contains(r.calls, "deploy") {
		t.Errorf("acted on a stale public key:\n%s\ncalls:\n%s", r.out, r.calls)
	}
	if !strings.Contains(r.out, "does not match its private key") {
		t.Errorf("output does not explain the mismatch:\n%s", r.out)
	}
}
