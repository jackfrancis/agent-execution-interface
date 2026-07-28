package dispatch

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// b64seg base64url-encodes a JSON object as a compact-JWS segment.
func b64seg(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// mintSVID signs a JWT-SVID with priv, as SPIRE would when issuing a JWT-SVID.
func mintSVID(t *testing.T, priv ed25519.PrivateKey, kid, sub string, aud any, exp int64) string {
	t.Helper()
	signingInput := b64seg(t, map[string]any{"alg": "EdDSA", "kid": kid, "typ": "JWT"}) + "." +
		b64seg(t, map[string]any{"sub": sub, "aud": aud, "exp": exp})
	sig := ed25519.Sign(priv, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// jwksFor builds a single-key SPIFFE JWT bundle (JWKS) for pub.
func jwksFor(t *testing.T, kid string, pub ed25519.PublicKey) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "kid": kid,
		"x": base64.RawURLEncoding.EncodeToString(pub),
	}}})
	if err != nil {
		t.Fatalf("marshal jwks: %v", err)
	}
	return b
}

// newAuth builds a SPIFFE authenticator with a fixed clock so expiry is deterministic.
func newAuth(t *testing.T, pub ed25519.PublicKey, now time.Time) *SPIFFEAuthenticator {
	t.Helper()
	bundle, err := ParseTrustBundle(jwksFor(t, "k1", pub))
	if err != nil {
		t.Fatalf("ParseTrustBundle: %v", err)
	}
	a := NewSPIFFEAuthenticator("example.org", "aei-dispatch", bundle)
	a.now = func() time.Time { return now }
	return a
}

// TestSPIFFEAuthenticateValidSVID: a well-formed JWT-SVID verifies offline (no
// apiserver) and maps the SPIFFE ID to the caller's {namespace, serviceaccount}.
func TestSPIFFEAuthenticateValidSVID(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	now := time.Unix(1_700_000_000, 0)
	a := newAuth(t, pub, now)

	svid := mintSVID(t, priv, "k1", "spiffe://example.org/ns/gofer/sa/gofer", "aei-dispatch", now.Add(time.Hour).Unix())
	caller, err := a.Authenticate(context.Background(), svid)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if caller.Namespace != "gofer" || caller.ServiceAccount != "gofer" {
		t.Fatalf("caller = %+v, want {gofer gofer}", caller)
	}
}

// TestSPIFFEAudienceArray: the JWT `aud` may be an array; membership is what matters.
func TestSPIFFEAudienceArray(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	now := time.Unix(1_700_000_000, 0)
	a := newAuth(t, pub, now)

	svid := mintSVID(t, priv, "k1", "spiffe://example.org/ns/team/sa/runner",
		[]string{"other", "aei-dispatch"}, now.Add(time.Hour).Unix())
	caller, err := a.Authenticate(context.Background(), svid)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if caller.Namespace != "team" || caller.ServiceAccount != "runner" {
		t.Fatalf("caller = %+v, want {team runner}", caller)
	}
}

// TestSPIFFERejects covers the ways a JWT-SVID must fail closed: wrong alg (an
// algorithm-confusion / "none" downgrade), expiry, audience mismatch, a foreign trust
// domain, a non-workload SPIFFE path, an unknown kid, and a tampered signature.
func TestSPIFFERejects(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	now := time.Unix(1_700_000_000, 0)
	a := newAuth(t, pub, now)
	good := "spiffe://example.org/ns/gofer/sa/gofer"
	hour := now.Add(time.Hour).Unix()

	// alg confusion: an HS256 header must be rejected (only EdDSA is accepted).
	hs256 := b64seg(t, map[string]any{"alg": "HS256", "kid": "k1"}) + "." +
		b64seg(t, map[string]any{"sub": good, "aud": "aei-dispatch", "exp": hour})
	hs256 += "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte(hs256)))

	// A validly-signed token whose signature bytes are then flipped. Tamper a decoded
	// signature byte (not trailing base64 chars, whose low bits are RawURLEncoding
	// padding and can decode unchanged), so the signature is always invalidated.
	tampered := mintSVID(t, priv, "k1", good, "aei-dispatch", hour)
	if dot := strings.LastIndexByte(tampered, '.'); dot >= 0 {
		sig, err := base64.RawURLEncoding.DecodeString(tampered[dot+1:])
		if err != nil || len(sig) == 0 {
			t.Fatalf("decode signature: %v", err)
		}
		sig[0] ^= 0xff // flip the first signature byte
		tampered = tampered[:dot+1] + base64.RawURLEncoding.EncodeToString(sig)
	}

	cases := map[string]string{
		"wrong-alg":         hs256,
		"expired":           mintSVID(t, priv, "k1", good, "aei-dispatch", now.Add(-time.Minute).Unix()),
		"audience-mismatch": mintSVID(t, priv, "k1", good, "someone-else", hour),
		"foreign-domain":    mintSVID(t, priv, "k1", "spiffe://evil.example/ns/gofer/sa/gofer", "aei-dispatch", hour),
		"non-workload-path": mintSVID(t, priv, "k1", "spiffe://example.org/some/thing", "aei-dispatch", hour),
		"unknown-kid":       mintSVID(t, priv, "k2", good, "aei-dispatch", hour),
		"tampered-sig":      tampered,
		"malformed":         "not.a.jwt.at.all",
	}
	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := a.Authenticate(context.Background(), tok); err == nil {
				t.Fatalf("%s: want rejection, got nil error", name)
			}
		})
	}
}

// TestParseTrustBundleSkipsNonEd25519: EC keys (production ES256 SVIDs) are skipped,
// not fatal, but a bundle with no usable key is an error.
func TestParseTrustBundleSkipsNonEd25519(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	mixed, _ := json.Marshal(map[string]any{"keys": []map[string]any{
		{"kty": "EC", "crv": "P-256", "kid": "ec", "x": "aa", "y": "bb"},
		{"kty": "OKP", "crv": "Ed25519", "kid": "ok", "x": base64.RawURLEncoding.EncodeToString(pub)},
	}})
	b, err := ParseTrustBundle(mixed)
	if err != nil {
		t.Fatalf("ParseTrustBundle: %v", err)
	}
	if _, ok := b["ok"]; !ok || len(b) != 1 {
		t.Fatalf("bundle = %v, want only the Ed25519 key", b)
	}

	if _, err := ParseTrustBundle([]byte(`{"keys":[{"kty":"EC","crv":"P-256","kid":"ec"}]}`)); err == nil {
		t.Fatal("a bundle with no Ed25519 key should error")
	}
}
