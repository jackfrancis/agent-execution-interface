package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

// entrypointResolver reads the process command (Entrypoint ++ Cmd) a container image
// declares, which the durable provider stamps onto the ActorTemplate. ate — unlike a
// normal container runtime or the isolated AEI substrates (k8s Job / agent-sandbox) —
// does NOT derive the OCI Process.Args from the image, so an ActorTemplate with no
// command yields empty Args and gVisor rejects it ("Spec.Process.Arg must be
// defined"). Resolving the command from the image keeps the durable contract
// identical to the isolated one: the app ships an image with an ENTRYPOINT and
// declares its command nowhere else (no CRD field, no per-substrate app code).
type entrypointResolver interface {
	resolve(ctx context.Context, imageRef string) ([]string, error)
}

// acceptManifests asks for both OCI and Docker image manifests and indexes, so the
// resolver handles single-arch images and multi-arch indexes from either tooling.
const acceptManifests = "application/vnd.oci.image.index.v1+json," +
	"application/vnd.oci.image.manifest.v1+json," +
	"application/vnd.docker.distribution.manifest.list.v2+json," +
	"application/vnd.docker.distribution.manifest.v2+json"

// registryEndpoint is the address the controller actually dials for a registry, which
// may differ from the host in the image ref. On kind the app image is published as
// localhost:5001/... (what the ate NODES pull via containerd), but the controller POD
// cannot reach that host; it dials the same registry in-cluster (e.g. an
// http://kind-registry.default.svc:5000 Service). insecure selects plain HTTP.
type registryEndpoint struct {
	host     string
	insecure bool
}

// registryResolver reads image configs over the registry HTTP API (v2). It adds no
// third-party dependency — the package stays net/http-only. Durable images are always
// digest-pinned, so it resolves the manifest + config for a single digest, following
// a Bearer-token challenge when the registry issues one (anonymous pulls from public
// registries) and an image index down to this platform's manifest.
type registryResolver struct {
	hc       *http.Client
	rewrites map[string]registryEndpoint
}

func newRegistryResolver(rewrites map[string]registryEndpoint) *registryResolver {
	return &registryResolver{hc: &http.Client{Timeout: 30 * time.Second}, rewrites: rewrites}
}

// resolve returns the image's Entrypoint ++ Cmd (the OCI process args), erroring if
// the image declares neither (a durable actor needs a process command).
func (r *registryResolver) resolve(ctx context.Context, imageRef string) ([]string, error) {
	registry, repo, id, err := parseImageRef(imageRef)
	if err != nil {
		return nil, err
	}
	host, scheme := r.endpoint(registry)
	s := &regSession{r: r, host: host, scheme: scheme, repo: repo}

	var man manifestDoc
	if err := s.get(ctx, "/manifests/"+id, acceptManifests, &man); err != nil {
		return nil, fmt.Errorf("fetch manifest %s: %w", imageRef, err)
	}
	// An index (multi-arch): pick this platform's manifest and fetch it.
	if len(man.Manifests) > 0 {
		d := pickPlatform(man.Manifests)
		if d == "" {
			return nil, fmt.Errorf("image index %s has no linux/%s manifest", imageRef, runtime.GOARCH)
		}
		man = manifestDoc{}
		if err := s.get(ctx, "/manifests/"+d, acceptManifests, &man); err != nil {
			return nil, fmt.Errorf("fetch platform manifest %s: %w", imageRef, err)
		}
	}
	if man.Config.Digest == "" {
		return nil, fmt.Errorf("manifest %s has no config blob", imageRef)
	}
	var cfg imageConfigDoc
	if err := s.get(ctx, "/blobs/"+man.Config.Digest, "application/json", &cfg); err != nil {
		return nil, fmt.Errorf("fetch image config %s: %w", imageRef, err)
	}
	cmd := append(append([]string{}, cfg.Config.Entrypoint...), cfg.Config.Cmd...)
	if len(cmd) == 0 {
		return nil, fmt.Errorf("image %s declares no entrypoint or cmd; a durable actor needs a process command (the image must define its runtime entrypoint, as the isolated substrates already require)", imageRef)
	}
	return cmd, nil
}

// endpoint resolves where to dial a registry: a configured rewrite wins (with its
// http/https), a bare localhost/127.0.0.1 registry is plain HTTP, everything else is
// HTTPS (the prod default — the image ref's own registry, reachable from the pod).
func (r *registryResolver) endpoint(registry string) (host, scheme string) {
	if ep, ok := r.rewrites[registry]; ok {
		if ep.insecure {
			return ep.host, "http"
		}
		return ep.host, "https"
	}
	if registry == "localhost" || strings.HasPrefix(registry, "localhost:") || strings.HasPrefix(registry, "127.0.0.1") {
		return registry, "http"
	}
	return registry, "https"
}

// regSession carries the dialed registry coordinates and a bearer token across the
// manifest + config requests for one image.
type regSession struct {
	r      *registryResolver
	host   string
	scheme string
	repo   string
	token  string
}

// get fetches url path (relative to /v2/<repo>) and decodes the JSON body into out.
func (s *regSession) get(ctx context.Context, path, accept string, out any) error {
	body, err := s.do(ctx, s.scheme+"://"+s.host+"/v2/"+s.repo+path, accept, true)
	if err != nil {
		return err
	}
	defer body.Close()
	return json.NewDecoder(body).Decode(out)
}

// do issues the GET, transparently answering a single Bearer-token challenge.
func (s *regSession) do(ctx context.Context, u, accept string, mayAuth bool) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.r.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized && mayAuth {
		challenge := resp.Header.Get("WWW-Authenticate")
		_ = resp.Body.Close()
		if err := s.authenticate(ctx, challenge); err != nil {
			return nil, fmt.Errorf("registry auth: %w", err)
		}
		return s.do(ctx, u, accept, false) // retry once with the token, no further auth
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s: %s", u, resp.Status, strings.TrimSpace(string(b)))
	}
	return resp.Body, nil
}

// authenticate handles a `Bearer realm=...,service=...,scope=...` challenge by
// fetching an anonymous token from the realm. Private-registry credentials (from the
// cluster's imagePullSecrets) would be set as Basic auth on this request — the prod
// extension point; anonymous covers a kind dev registry and public registries.
func (s *regSession) authenticate(ctx context.Context, challenge string) error {
	realm, params := parseChallenge(challenge)
	if realm == "" {
		return fmt.Errorf("unsupported auth challenge %q", challenge)
	}
	u, err := url.Parse(realm)
	if err != nil {
		return fmt.Errorf("parse token realm: %w", err)
	}
	q := u.Query()
	if v := params["service"]; v != "" {
		q.Set("service", v)
	}
	if v := params["scope"]; v != "" {
		q.Set("scope", v)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := s.r.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token endpoint %s: %s", u.Host, resp.Status)
	}
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return fmt.Errorf("decode token: %w", err)
	}
	if s.token = tok.Token; s.token == "" {
		s.token = tok.AccessToken
	}
	if s.token == "" {
		return fmt.Errorf("token endpoint %s returned no token", u.Host)
	}
	return nil
}

// parseImageRef splits a (digest-pinned) image ref into registry, repository, and the
// digest/tag reference. A first path segment that looks like a host (has '.'/':' or is
// localhost) is the registry; otherwise the ref is a Docker Hub short name.
func parseImageRef(ref string) (registry, repo, id string, err error) {
	name := ref
	if i := strings.LastIndex(ref, "@"); i >= 0 {
		name, id = ref[:i], ref[i+1:]
	} else if i := strings.LastIndex(ref, ":"); i >= 0 && !strings.Contains(ref[i+1:], "/") {
		name, id = ref[:i], ref[i+1:] // a tag (durable requires a digest, but support both)
	}
	if name == "" || id == "" {
		return "", "", "", fmt.Errorf("image ref %q: expected <registry>/<repo>@<digest>", ref)
	}
	slash := strings.IndexByte(name, '/')
	if slash < 0 || !isRegistryHost(name[:slash]) {
		repo = name // Docker Hub short/official ref
		if !strings.Contains(repo, "/") {
			repo = "library/" + repo
		}
		return "registry-1.docker.io", repo, id, nil
	}
	return name[:slash], name[slash+1:], id, nil
}

// isRegistryHost reports whether the first ref segment is a registry host rather than
// the first element of a Docker Hub repository path.
func isRegistryHost(s string) bool {
	return s == "localhost" || strings.ContainsAny(s, ".:")
}

// pickPlatform selects this platform's manifest digest from an index, preferring
// linux/<GOARCH>, then any linux manifest, then the first entry. The entrypoint is
// almost always identical across an image's architectures, so this only matters for
// odd multi-arch images with per-arch entrypoints.
func pickPlatform(ms []manifestEntry) string {
	for _, m := range ms {
		if m.Platform.OS == "linux" && m.Platform.Architecture == runtime.GOARCH {
			return m.Digest
		}
	}
	for _, m := range ms {
		if m.Platform.OS == "linux" {
			return m.Digest
		}
	}
	if len(ms) > 0 {
		return ms[0].Digest
	}
	return ""
}

// parseChallenge parses a `Bearer realm="...",service="...",scope="..."` header into
// the realm and the remaining parameters.
func parseChallenge(h string) (realm string, params map[string]string) {
	params = map[string]string{}
	h = strings.TrimSpace(h)
	if !strings.HasPrefix(strings.ToLower(h), "bearer ") {
		return "", params
	}
	for _, part := range splitOutsideQuotes(h[len("Bearer "):]) {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"`)
		if k == "realm" {
			realm = v
		} else {
			params[k] = v
		}
	}
	return realm, params
}

// splitOutsideQuotes splits s on commas that are not inside double quotes (a scope
// value like `repository:x:pull,push` contains a comma).
func splitOutsideQuotes(s string) []string {
	var out []string
	var b strings.Builder
	inQuote := false
	for _, c := range s {
		switch {
		case c == '"':
			inQuote = !inQuote
			b.WriteRune(c)
		case c == ',' && !inQuote:
			out = append(out, b.String())
			b.Reset()
		default:
			b.WriteRune(c)
		}
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out
}

// parseRegistryRewrites reads AEI_REGISTRY_REWRITE — comma-separated <from>=<to> pairs
// that redirect where the controller reads an image's config (the ActorTemplate still
// carries the original ref, which the ate nodes pull). A <to> of http://host is plain
// HTTP (a kind dev registry); https://host or a bare host is HTTPS. Empty ⇒ no
// rewrites: the controller dials each ref's own registry, the production default.
func parseRegistryRewrites(env string) map[string]registryEndpoint {
	m := map[string]registryEndpoint{}
	for _, pair := range strings.Split(env, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		from, to, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		from, to = strings.TrimSpace(from), strings.TrimSpace(to)
		ep := registryEndpoint{host: to}
		switch {
		case strings.HasPrefix(to, "http://"):
			ep.host, ep.insecure = strings.TrimPrefix(to, "http://"), true
		case strings.HasPrefix(to, "https://"):
			ep.host = strings.TrimPrefix(to, "https://")
		}
		if from != "" && ep.host != "" {
			m[from] = ep
		}
	}
	return m
}

type manifestDoc struct {
	MediaType string `json:"mediaType"`
	Config    struct {
		Digest string `json:"digest"`
	} `json:"config"`
	Manifests []manifestEntry `json:"manifests"`
}

type manifestEntry struct {
	Digest   string `json:"digest"`
	Platform struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
	} `json:"platform"`
}

type imageConfigDoc struct {
	Config struct {
		Entrypoint []string `json:"Entrypoint"`
		Cmd        []string `json:"Cmd"`
	} `json:"config"`
}
