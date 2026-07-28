package dispatch

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TrustBundle maps a JWT key id (kid) to an Ed25519 public verification key: the
// offline analog of the apiserver's token check. It is a SPIFFE trust bundle for one
// trust domain, distributed out of band (SPIRE publishes it) and refreshed on key
// rotation — so verifying a caller needs no network call.
type TrustBundle map[string]ed25519.PublicKey

// SPIFFEAuthenticator verifies a caller's JWT-SVID offline against a trust bundle,
// with NO Kubernetes TokenReview — removing the apiserver round-trip the reference
// tokenReviewer makes on every dispatch, the last hot-path control-plane dependency
// on the apiserver (roadmap Phase 6). It accepts only EdDSA (the bundle holds Ed25519
// keys), checks expiry and audience, and maps the SPIFFE ID in `sub` —
// spiffe://<trust-domain>/ns/<ns>/sa/<sa>, the SPIRE Kubernetes workload shape — to
// the same {namespace, serviceaccount} Caller the TokenReview path yields.
//
// It is the offline instantiation of the Authenticator seam: the same interface as
// tokenReviewer, so main() swaps it in by configuration. A production profile verifies
// SPIRE-issued ES256 SVIDs through the same seam (go-spiffe); the reference keeps to
// EdDSA to stay dependency-free. The tradeoff is deliberate: offline verification
// trades the apiserver's instant revocation for availability, recovered with short
// SVID TTLs and bundle refresh.
type SPIFFEAuthenticator struct {
	trustDomain string
	audience    string
	bundle      TrustBundle
	now         func() time.Time
}

// NewSPIFFEAuthenticator builds an offline authenticator for a trust domain. audience
// is the value a caller's SVID must be bound to (the dispatch endpoint's identity);
// bundle is the trust domain's Ed25519 verification keys by kid.
func NewSPIFFEAuthenticator(trustDomain, audience string, bundle TrustBundle) *SPIFFEAuthenticator {
	return &SPIFFEAuthenticator{trustDomain: trustDomain, audience: audience, bundle: bundle, now: time.Now}
}

// Authenticate verifies the caller's JWT-SVID and returns its workload identity,
// entirely offline.
func (a *SPIFFEAuthenticator) Authenticate(_ context.Context, bearer string) (Caller, error) {
	claims, err := a.verify(bearer)
	if err != nil {
		return Caller{}, err
	}
	return callerFromSPIFFEID(claims.subject, a.trustDomain)
}

// svidClaims is the subset of a JWT-SVID's claims this authenticator checks.
type svidClaims struct {
	subject  string
	audience []string
	expiry   int64
}

// verify checks the compact-JWS signature against the trust bundle and validates the
// registered claims. It accepts only alg "EdDSA" and only keys from the bundle, so
// there is no path for an "alg":"none" or an algorithm-confusion downgrade.
func (a *SPIFFEAuthenticator) verify(token string) (svidClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return svidClaims{}, errors.New("dispatch: malformed JWT-SVID")
	}

	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := decodeSegment(parts[0], &hdr); err != nil {
		return svidClaims{}, fmt.Errorf("dispatch: JWT-SVID header: %w", err)
	}
	if hdr.Alg != "EdDSA" {
		return svidClaims{}, fmt.Errorf("dispatch: unsupported JWT-SVID alg %q (want EdDSA)", hdr.Alg)
	}
	pub, ok := a.bundle[hdr.Kid]
	if !ok {
		return svidClaims{}, fmt.Errorf("dispatch: no trust-bundle key for kid %q", hdr.Kid)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return svidClaims{}, fmt.Errorf("dispatch: JWT-SVID signature: %w", err)
	}
	if !ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig) {
		return svidClaims{}, errors.New("dispatch: JWT-SVID signature invalid")
	}

	var raw struct {
		Sub string          `json:"sub"`
		Aud json.RawMessage `json:"aud"`
		Exp int64           `json:"exp"`
	}
	if err := decodeSegment(parts[1], &raw); err != nil {
		return svidClaims{}, fmt.Errorf("dispatch: JWT-SVID claims: %w", err)
	}
	if raw.Exp == 0 || a.now().Unix() >= raw.Exp {
		return svidClaims{}, errors.New("dispatch: JWT-SVID expired")
	}
	aud, err := parseAudience(raw.Aud)
	if err != nil {
		return svidClaims{}, err
	}
	if !contains(aud, a.audience) {
		return svidClaims{}, fmt.Errorf("dispatch: JWT-SVID audience %v does not include %q", aud, a.audience)
	}
	return svidClaims{subject: raw.Sub, audience: aud, expiry: raw.Exp}, nil
}

// callerFromSPIFFEID maps a SPIFFE ID to a Caller. It accepts the SPIRE Kubernetes
// workload shape spiffe://<trust-domain>/ns/<namespace>/sa/<serviceaccount>, so the
// offline path yields the same {namespace, serviceaccount} Caller the TokenReview path
// extracts from system:serviceaccount:<ns>:<sa>.
func callerFromSPIFFEID(id, trustDomain string) (Caller, error) {
	rest, ok := strings.CutPrefix(id, "spiffe://")
	if !ok {
		return Caller{}, fmt.Errorf("dispatch: subject %q is not a SPIFFE ID", id)
	}
	host, path, _ := strings.Cut(rest, "/")
	if host != trustDomain {
		return Caller{}, fmt.Errorf("dispatch: SVID trust domain %q, want %q", host, trustDomain)
	}
	seg := strings.Split(path, "/")
	if len(seg) != 4 || seg[0] != "ns" || seg[2] != "sa" || seg[1] == "" || seg[3] == "" {
		return Caller{}, fmt.Errorf("dispatch: SPIFFE ID path %q is not ns/<ns>/sa/<sa>", path)
	}
	return Caller{Namespace: seg[1], ServiceAccount: seg[3]}, nil
}

// parseAudience decodes the JWT `aud` claim, which is a string or an array of strings.
func parseAudience(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("dispatch: JWT-SVID has no audience")
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		return many, nil
	}
	return nil, errors.New("dispatch: JWT-SVID audience is neither string nor array")
}

// decodeSegment base64url-decodes one compact-JWS segment into v.
func decodeSegment(seg string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// ParseTrustBundle reads a SPIFFE JWT trust bundle (a JWKS) and returns its Ed25519
// keys by kid. It supports OKP (Ed25519) keys — the dependency-free reference set; a
// production bundle also carries EC (ES256) keys, which the SPIFFE profile verifies
// through a JWT library. Non-Ed25519 keys are skipped, not an error.
func ParseTrustBundle(jwks []byte) (TrustBundle, error) {
	var doc struct {
		Keys []struct {
			Kty string `json:"kty"`
			Crv string `json:"crv"`
			Kid string `json:"kid"`
			X   string `json:"x"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(jwks, &doc); err != nil {
		return nil, fmt.Errorf("dispatch: parse trust bundle: %w", err)
	}
	bundle := TrustBundle{}
	for _, k := range doc.Keys {
		if k.Kty != "OKP" || k.Crv != "Ed25519" {
			continue // EC/ES256 etc.: production-profile algs, skipped by the reference
		}
		raw, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return nil, fmt.Errorf("dispatch: trust-bundle key %q: %w", k.Kid, err)
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("dispatch: trust-bundle key %q: bad Ed25519 length %d", k.Kid, len(raw))
		}
		bundle[k.Kid] = ed25519.PublicKey(raw)
	}
	if len(bundle) == 0 {
		return nil, errors.New("dispatch: trust bundle has no Ed25519 keys")
	}
	return bundle, nil
}
