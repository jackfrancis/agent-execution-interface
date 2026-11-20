package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/jackfrancis/agent-execution-interface/control"
)

// minHMACSecretBytes is the shortest HMAC signing secret the controller accepts.
const minHMACSecretBytes = 32

// placeholderSecrets are values that shipped in, or are typical of, example
// manifests. A secret containing one is a template nobody filled in.
var placeholderSecrets = []string{"replace-me", "replace_me", "changeme", "change-me", "placeholder"}

// errNoSigningKey is the startup error when no usable key is configured.
var errNoSigningKey = errors.New("no run-credential signing key configured: " +
	"set AEI_ED25519_PRIVATE_KEY (run 'make authority' to provision one in the aei-signing secret) " +
	"or AEI_SIGNING_SECRET (at least 32 random bytes)")

// configureSigning sets cfg's run-credential authority from the environment and
// returns a description of it for the startup log. It fails closed: with no key,
// a malformed Ed25519 key, or an empty, short or placeholder HMAC secret it
// returns an error and the controller must not start — otherwise every run would
// carry a forgeable or empty credential.
//
// AEI_ED25519_PRIVATE_KEY (asymmetric, sole minter; what 'make authority'
// provisions) takes precedence over AEI_SIGNING_SECRET (reference HMAC).
func configureSigning(cfg *control.Config, getenv func(string) string) (string, error) {
	if k := strings.TrimSpace(getenv("AEI_ED25519_PRIVATE_KEY")); k != "" {
		raw, err := base64.StdEncoding.DecodeString(k)
		if err != nil {
			return "", fmt.Errorf("AEI_ED25519_PRIVATE_KEY is not valid base64: %w", err)
		}
		if len(raw) != ed25519.PrivateKeySize {
			return "", fmt.Errorf("AEI_ED25519_PRIVATE_KEY is %d bytes; want a %d-byte Ed25519 private key", len(raw), ed25519.PrivateKeySize)
		}
		// A Go Ed25519 private key is seed || public key. Signing uses the seed and
		// embeds the second half in the signature computation, so a mismatched half
		// mints credentials that the advertised public key cannot verify.
		priv := ed25519.PrivateKey(raw)
		if !bytes.Equal(ed25519.NewKeyFromSeed(priv.Seed()), priv) {
			return "", errors.New("AEI_ED25519_PRIVATE_KEY is inconsistent: its public half does not match its seed")
		}
		cfg.Authority = control.NewEd25519Authority(priv)
		return "Ed25519 (asymmetric, sole-minter)", nil
	}
	secret := getenv("AEI_SIGNING_SECRET")
	if strings.TrimSpace(secret) == "" {
		return "", errNoSigningKey
	}
	lower := strings.ToLower(secret)
	for _, p := range placeholderSecrets {
		if strings.Contains(lower, p) {
			return "", fmt.Errorf("AEI_SIGNING_SECRET is a placeholder (%q); %w", p, errNoSigningKey)
		}
	}
	if len(secret) < minHMACSecretBytes {
		return "", fmt.Errorf("AEI_SIGNING_SECRET is %d bytes; want at least %d", len(secret), minHMACSecretBytes)
	}
	cfg.Secret = []byte(secret)
	return "HMAC (reference-grade, symmetric)", nil
}
