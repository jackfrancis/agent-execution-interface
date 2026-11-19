package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/control"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestConfigureSigning(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	edKey := base64.StdEncoding.EncodeToString(priv)
	strong := base64.StdEncoding.EncodeToString(make([]byte, 32)) // 44 bytes
	// The right seed with another key's public half: 64 bytes, but inconsistent.
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bad := append(append([]byte{}, priv.Seed()...), otherPub...)
	mismatched := base64.StdEncoding.EncodeToString(bad)

	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string // substring; "" means success
		wantEd  bool
	}{
		{name: "nothing configured", env: nil, wantErr: "no run-credential signing key configured"},
		{name: "blank secret", env: map[string]string{"AEI_SIGNING_SECRET": "   "}, wantErr: "no run-credential signing key configured"},
		{name: "shipped placeholder", env: map[string]string{"AEI_SIGNING_SECRET": "replace-me-with-32-or-more-random-bytes"}, wantErr: "placeholder"},
		{name: "changeme placeholder", env: map[string]string{"AEI_SIGNING_SECRET": "CHANGEME-CHANGEME-CHANGEME-CHANGEME"}, wantErr: "placeholder"},
		{name: "short secret", env: map[string]string{"AEI_SIGNING_SECRET": "too-short"}, wantErr: "want at least 32"},
		{name: "ed25519 not base64", env: map[string]string{"AEI_ED25519_PRIVATE_KEY": "!!!"}, wantErr: "not valid base64"},
		{name: "ed25519 wrong size", env: map[string]string{"AEI_ED25519_PRIVATE_KEY": base64.StdEncoding.EncodeToString([]byte("short"))}, wantErr: "want a 64-byte"},
		{name: "ed25519 mismatched public half", env: map[string]string{"AEI_ED25519_PRIVATE_KEY": mismatched}, wantErr: "public half does not match its seed"},
		{name: "strong hmac secret", env: map[string]string{"AEI_SIGNING_SECRET": strong}},
		{name: "ed25519 key", env: map[string]string{"AEI_ED25519_PRIVATE_KEY": edKey}, wantEd: true},
		{name: "ed25519 wins over placeholder secret", env: map[string]string{"AEI_ED25519_PRIVATE_KEY": edKey, "AEI_SIGNING_SECRET": "replace-me"}, wantEd: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg control.Config
			kind, err := configureSigning(&cfg, envOf(tc.env))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v; want one containing %q", err, tc.wantErr)
				}
				if cfg.Authority != nil || cfg.Secret != nil {
					t.Error("a rejected configuration must leave cfg unkeyed")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantEd != (cfg.Authority != nil) {
				t.Errorf("Ed25519 authority set = %v; want %v (kind %q)", cfg.Authority != nil, tc.wantEd, kind)
			}
			// Whatever was accepted must actually mint a run credential.
			l := &credCapture{}
			if _, err := control.New(cfg).Run(context.Background(), l, aei.RunSpec{ID: "r"}); err != nil {
				t.Fatalf("Run with accepted config: %v", err)
			}
			if l.cred.Value == "" {
				t.Error("accepted config dispatched an empty credential")
			}
		})
	}
	_, err = configureSigning(&control.Config{}, envOf(map[string]string{"AEI_SIGNING_SECRET": "replace-me-xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}))
	if !errors.Is(err, errNoSigningKey) {
		t.Errorf("a placeholder error should wrap errNoSigningKey, got %v", err)
	}
}

type credCapture struct{ cred aei.Credential }

func (c *credCapture) Launch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	c.cred = cred
	return aei.Handle{Ref: spec.ID}, nil
}

// The controller binary itself exits non-zero with a clear message when no
// signing key is configured — before it touches the cluster.
func TestManagerExitsWithoutSigningKey(t *testing.T) {
	if os.Getenv("AEI_TEST_RUN_MAIN") == "1" {
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestManagerExitsWithoutSigningKey$")
	cmd.Env = []string{"AEI_TEST_RUN_MAIN=1", "HOME=" + os.Getenv("HOME"), "KUBECONFIG=/nonexistent"}
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("manager exit = %v; want exit status 1\n%s", err, out)
	}
	for _, want := range []string{"refusing to start", "no run-credential signing key configured", "make authority"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}
