package substrate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

// insecureRewrite points a registry host at an httptest server (plain HTTP).
func insecureRewrite(from, tsURL string) map[string]registryEndpoint {
	return map[string]registryEndpoint{from: {host: strings.TrimPrefix(tsURL, "http://"), insecure: true}}
}

// TestRegistryResolverEntrypoint: a single-arch image resolves to Entrypoint ++ Cmd.
func TestRegistryResolverEntrypoint(t *testing.T) {
	const digest = "sha256:aaa"
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/gofer-runtime/manifests/"+digest, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{"digest": "sha256:cfg"}})
	})
	mux.HandleFunc("/v2/gofer-runtime/blobs/sha256:cfg", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{
			"Entrypoint": []string{"/runtime"}, "Cmd": []string{"--serve"},
		}})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	r := newRegistryResolver(insecureRewrite("localhost:5001", ts.URL))
	cmd, err := r.resolve(context.Background(), "localhost:5001/gofer-runtime@"+digest)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(cmd) != 2 || cmd[0] != "/runtime" || cmd[1] != "--serve" {
		t.Errorf("cmd = %v, want [/runtime --serve]", cmd)
	}
}

// TestRegistryResolverTokenChallenge: the resolver answers a Bearer-token challenge
// (an anonymous pull from a public-style registry) and retries with the token.
func TestRegistryResolverTokenChallenge(t *testing.T) {
	const digest = "sha256:bbb"
	var baseURL string
	authed := false
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope") == "" {
			t.Errorf("token request missing scope")
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"token": "tok"})
	})
	mux.HandleFunc("/v2/o/r/manifests/"+digest, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+baseURL+`/token",service="reg",scope="repository:o/r:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		authed = true
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{"digest": "sha256:c"}})
	})
	mux.HandleFunc("/v2/o/r/blobs/sha256:c", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{"Entrypoint": []string{"/x"}}})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	baseURL = ts.URL

	r := newRegistryResolver(insecureRewrite("reg.example", ts.URL))
	cmd, err := r.resolve(context.Background(), "reg.example/o/r@"+digest)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !authed || len(cmd) != 1 || cmd[0] != "/x" {
		t.Errorf("cmd = %v authed = %v, want [/x] authed", cmd, authed)
	}
}

// TestRegistryResolverIndex: a multi-arch index resolves down to this platform's
// manifest before reading the config.
func TestRegistryResolverIndex(t *testing.T) {
	const digest = "sha256:idx"
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/app/manifests/"+digest, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"manifests": []map[string]any{
			{"digest": "sha256:other", "platform": map[string]string{"os": "linux", "architecture": "mips"}},
			{"digest": "sha256:mine", "platform": map[string]string{"os": "linux", "architecture": runtime.GOARCH}},
		}})
	})
	mux.HandleFunc("/v2/app/manifests/sha256:mine", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{"digest": "sha256:c"}})
	})
	mux.HandleFunc("/v2/app/blobs/sha256:c", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{"Entrypoint": []string{"/app"}}})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	r := newRegistryResolver(insecureRewrite("reg.io", ts.URL))
	cmd, err := r.resolve(context.Background(), "reg.io/app@"+digest)
	if err != nil || len(cmd) != 1 || cmd[0] != "/app" {
		t.Errorf("cmd = %v, err = %v, want [/app]", cmd, err)
	}
}

// TestRegistryResolverNoEntrypoint: an image that declares neither Entrypoint nor Cmd
// is an error (a durable actor needs a process command).
func TestRegistryResolverNoEntrypoint(t *testing.T) {
	const digest = "sha256:ccc"
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/bare/manifests/"+digest, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{"digest": "sha256:c"}})
	})
	mux.HandleFunc("/v2/bare/blobs/sha256:c", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{}})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	r := newRegistryResolver(insecureRewrite("reg.io", ts.URL))
	if _, err := r.resolve(context.Background(), "reg.io/bare@"+digest); err == nil {
		t.Fatal("want an error for an image with no entrypoint or cmd, got nil")
	}
}

// TestParseImageRef covers registry detection (explicit host vs Docker Hub short ref).
func TestParseImageRef(t *testing.T) {
	cases := []struct{ ref, reg, repo, id string }{
		{"localhost:5001/gofer@sha256:abc", "localhost:5001", "gofer", "sha256:abc"},
		{"ghcr.io/acme/gofer-runtime@sha256:d", "ghcr.io", "acme/gofer-runtime", "sha256:d"},
		{"nginx@sha256:x", "registry-1.docker.io", "library/nginx", "sha256:x"},
		{"reg.io/team/app/sub@sha256:y", "reg.io", "team/app/sub", "sha256:y"},
	}
	for _, c := range cases {
		reg, repo, id, err := parseImageRef(c.ref)
		if err != nil || reg != c.reg || repo != c.repo || id != c.id {
			t.Errorf("parseImageRef(%q) = (%q,%q,%q,%v), want (%q,%q,%q)", c.ref, reg, repo, id, err, c.reg, c.repo, c.id)
		}
	}
}

// TestParseRegistryRewrites covers the AEI_REGISTRY_REWRITE env parsing.
func TestParseRegistryRewrites(t *testing.T) {
	m := parseRegistryRewrites("localhost:5001=http://kind-registry.default.svc:5000, ghcr.io=https://ghcr.io")
	if ep := m["localhost:5001"]; ep.host != "kind-registry.default.svc:5000" || !ep.insecure {
		t.Errorf("localhost rewrite = %+v, want insecure kind-registry.default.svc:5000", ep)
	}
	if ep := m["ghcr.io"]; ep.host != "ghcr.io" || ep.insecure {
		t.Errorf("ghcr rewrite = %+v, want secure ghcr.io", ep)
	}
	if len(parseRegistryRewrites("")) != 0 {
		t.Error("empty env should yield no rewrites")
	}
}
