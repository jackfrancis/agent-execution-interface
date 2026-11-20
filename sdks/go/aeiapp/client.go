// Package aeiapp is the Go app-developer SDK for the Agent Execution Interface.
// An app dispatches a run to the AEI dispatch API and reads its lifecycle back;
// the transport is a small HTTP/JSON contract (docs/dispatch-plane.md), so an app
// imports no provider and no client-go, and creates no per-run Kubernetes object.
// It is the Go peer of sdks/python — the same two-call surface (Dispatch, Wait) —
// and depends only on the standard library.
package aeiapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	base = "/aei/v1alpha1"
	// DefaultTokenPath is the pod's default ServiceAccount token, used when no
	// token path is configured.
	DefaultTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" // #nosec G101 -- path, not a secret
	// TokenPathEnv names the environment variable that points the client at a
	// projected ServiceAccount token, such as one minted for the dispatch API's
	// audience ("aei-dispatch").
	TokenPathEnv = "AEI_DISPATCH_TOKEN_PATH" // #nosec G101 -- variable name, not a secret
)

// ErrOverloaded is returned (wrapped) by Dispatch when AEI refuses the run because
// its dispatch queue is full (HTTP 429). Nothing was dispatched; retry after a
// back-off. Test for it with errors.Is.
var ErrOverloaded = errors.New("aeiapp: dispatch overloaded")

// ErrTokenFile is returned (wrapped, with the path and the OS error) when the
// token file set by WithTokenPath or AEI_DISPATCH_TOKEN_PATH cannot be read, such
// as a projected volume that is not mounted. Nothing is sent. Test for it with
// errors.Is.
var ErrTokenFile = errors.New("aeiapp: read token file")

// Identity is the delegated authorization a run requests: the acting subject and
// the scopes. The dispatch endpoint bounds these by the app's AgentApp policy.
type Identity struct {
	Subject string   `json:"subject,omitempty"`
	Scopes  []string `json:"scopes,omitempty"`
}

// Spec is the app's run intent — the dispatch body. Its JSON is the AgentRun spec
// shape (the published CRD schema), so the wire contract is that one schema.
type Spec struct {
	TaskRef        string            `json:"taskRef,omitempty"`
	Parameters     map[string]string `json:"parameters,omitempty"`
	Identity       Identity          `json:"identity,omitempty"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
}

// Timing is the launcher-symmetric run-timing breakdown read from status.
type Timing struct {
	ProvisioningSeconds float64 `json:"provisioningSeconds"`
	RuntimeSeconds      float64 `json:"runtimeSeconds"`
	ModelSeconds        float64 `json:"modelSeconds"`
	ModelCalls          int     `json:"modelCalls"`
	ToolCalls           int     `json:"toolCalls"`
}

// Result is the observed lifecycle of a run. The result payload itself lands in
// the app's own store (ResultRef is an opaque pointer AEI never parses); AEI
// carries only lifecycle.
type Result struct {
	Phase     string `json:"phase"`
	Message   string `json:"message"`
	ResultRef string `json:"resultRef"`
	Timing    Timing `json:"timing"`
}

// Succeeded reports whether the run finished successfully.
func (r Result) Succeeded() bool { return r.Phase == "Succeeded" }

// Done reports whether the run reached a terminal phase.
func (r Result) Done() bool { return r.Phase == "Succeeded" || r.Phase == "Failed" }

// Client dispatches runs for one app to the AEI dispatch API.
type Client struct {
	endpoint  string
	app       string
	token     string
	tokenPath string
	// tokenPathSet records that tokenPath was configured, not defaulted, so a
	// failure to read it is an error rather than an empty bearer.
	tokenPathSet bool
	http         *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithToken sets the bearer explicitly. It takes precedence over any token path.
func WithToken(t string) Option { return func(c *Client) { c.token = t } }

// WithTokenPath reads the bearer from the file at path, such as a ServiceAccount
// token projected for the dispatch API's audience. The client re-reads the file on
// every request, so a token the kubelet rotates is picked up. Without it the path
// comes from AEI_DISPATCH_TOKEN_PATH, else DefaultTokenPath. A configured path that
// cannot be read fails the request with ErrTokenFile; an unreadable default path
// sends no bearer.
func WithTokenPath(path string) Option { return func(c *Client) { c.tokenPath = path } }

// WithHTTPClient injects an HTTP client (tests, custom transport/TLS).
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.http = hc } }

// New builds a Client for app at the dispatch endpoint, e.g.
// "http://aei-controller.aei-system.svc:8081".
func New(endpoint, app string, opts ...Option) *Client {
	c := &Client{endpoint: strings.TrimRight(endpoint, "/"), app: app, http: http.DefaultClient}
	for _, o := range opts {
		o(c)
	}
	if c.tokenPath == "" {
		c.tokenPath = strings.TrimSpace(os.Getenv(TokenPathEnv))
	}
	c.tokenPathSet = c.tokenPath != ""
	if !c.tokenPathSet {
		c.tokenPath = DefaultTokenPath
	}
	return c
}

// Dispatch submits a run and returns its id. No Kubernetes object is created — the
// run exists only as control-plane state addressable by this id.
func (c *Client) Dispatch(ctx context.Context, spec Spec) (string, error) {
	// App plus the inlined Spec fields = the dispatch body.
	body, _ := json.Marshal(struct {
		App string `json:"app"`
		Spec
	}{App: c.app, Spec: spec})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+base+"/dispatch", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := c.authorize(req); err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return "", fmt.Errorf("%w (retry after %qs)", ErrOverloaded, resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("aeiapp: dispatch: status %d", resp.StatusCode)
	}
	var out struct {
		RunID string `json:"runId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.RunID, nil
}

// Get reads a run's current lifecycle.
func (c *Client) Get(ctx context.Context, runID string) (Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+base+"/runs/"+runID, nil)
	if err != nil {
		return Result{}, err
	}
	if err := c.authorize(req); err != nil {
		return Result{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("aeiapp: get run: status %d", resp.StatusCode)
	}
	var r Result
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return Result{}, err
	}
	return r, nil
}

// Wait polls a run until it reaches a terminal phase or ctx is done. It tracks
// completion only; the run's result lands in the app's own store. A failed poll is
// retried at the next tick, except an unreadable token file (ErrTokenFile), which
// retrying cannot fix.
func (c *Client) Wait(ctx context.Context, runID string, poll time.Duration) (Result, error) {
	if poll <= 0 {
		poll = 2 * time.Second
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		r, err := c.Get(ctx, runID)
		if err == nil && r.Done() {
			return r, nil
		}
		if errors.Is(err, ErrTokenFile) {
			return Result{Phase: "Failed", Message: err.Error()}, err
		}
		select {
		case <-ctx.Done():
			return Result{Phase: "Failed", Message: "aeiapp: " + ctx.Err().Error()}, ctx.Err()
		case <-t.C:
		}
	}
}

func (c *Client) authorize(req *http.Request) error {
	b, err := c.bearer()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+b)
	return nil
}

func (c *Client) bearer() (string, error) {
	if c.token != "" {
		return c.token, nil
	}
	b, err := os.ReadFile(c.tokenPath)
	if err != nil {
		if c.tokenPathSet {
			return "", fmt.Errorf("%w %s: %w", ErrTokenFile, c.tokenPath, err)
		}
		return "", nil // off-cluster with no token configured: send no bearer
	}
	return strings.TrimSpace(string(b)), nil
}
