package aeiapp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDispatchAndWait drives the client against a fake dispatch API: it must send
// the app + spec on POST /dispatch and read lifecycle from GET /runs/{id}.
func TestDispatchAndWait(t *testing.T) {
	var gotApp, gotSubject, gotBearer string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /aei/v1alpha1/dispatch", func(w http.ResponseWriter, r *http.Request) {
		gotBearer = r.Header.Get("Authorization")
		var body struct {
			App      string `json:"app"`
			TaskRef  string `json:"taskRef"`
			Identity struct {
				Subject string `json:"subject"`
			} `json:"identity"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotApp, gotSubject = body.App, body.Identity.Subject
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"runId": "run-xyz"})
	})
	mux.HandleFunc("GET /aei/v1alpha1/runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "run-xyz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(Result{Phase: "Succeeded", ResultRef: "store://hello"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(srv.URL, "pyfer", WithToken("sa-token"))
	runID, err := c.Dispatch(context.Background(), Spec{
		TaskRef:        "item://hello",
		Parameters:     map[string]string{"kind": "hello"},
		Identity:       Identity{Subject: "github:1", Scopes: []string{"signals:read"}},
		TimeoutSeconds: 60,
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if runID != "run-xyz" {
		t.Fatalf("runID = %q, want run-xyz", runID)
	}
	if gotApp != "pyfer" || gotSubject != "github:1" || gotBearer != "Bearer sa-token" {
		t.Fatalf("server saw app=%q subject=%q bearer=%q", gotApp, gotSubject, gotBearer)
	}

	res, err := c.Wait(context.Background(), runID, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if !res.Succeeded() || res.ResultRef != "store://hello" {
		t.Fatalf("result = %+v, want Succeeded store://hello", res)
	}
}

// TestDispatchOverloaded: a 429 from the dispatch API surfaces as ErrOverloaded, so
// a caller can back off and retry instead of treating it as a hard failure.
func TestDispatchOverloaded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "dispatch queue full", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := New(srv.URL, "app", WithToken("t"))
	if _, err := c.Dispatch(context.Background(), Spec{TaskRef: "x"}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Dispatch err = %v, want ErrOverloaded", err)
	}
}

// TestTokenPath: the bearer comes from WithToken, else the WithTokenPath file, else
// the file AEI_DISPATCH_TOKEN_PATH names, else the default ServiceAccount token, and
// the file is re-read per request so a rotated token is used.
func TestTokenPath(t *testing.T) {
	dir := t.TempDir()
	write := func(name, tok string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(tok+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	optPath := write("opt", "from-option")
	envPath := write("env", "from-env")

	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(Result{Phase: "Running"})
	}))
	defer srv.Close()
	bearer := func(c *Client) string {
		t.Helper()
		if _, err := c.Get(context.Background(), "r"); err != nil {
			t.Fatal(err)
		}
		return strings.TrimPrefix(got, "Bearer ")
	}

	t.Setenv(TokenPathEnv, envPath)
	if b := bearer(New(srv.URL, "a", WithTokenPath(optPath))); b != "from-option" {
		t.Errorf("WithTokenPath: bearer = %q", b)
	}
	if b := bearer(New(srv.URL, "a", WithTokenPath(optPath), WithToken("explicit"))); b != "explicit" {
		t.Errorf("WithToken over WithTokenPath: bearer = %q", b)
	}
	c := New(srv.URL, "a")
	if b := bearer(c); b != "from-env" {
		t.Errorf("%s: bearer = %q", TokenPathEnv, b)
	}
	write("env", "rotated")
	if b := bearer(c); b != "rotated" {
		t.Errorf("after rotation: bearer = %q", b)
	}
	t.Setenv(TokenPathEnv, "")
	if c := New(srv.URL, "a"); c.tokenPath != DefaultTokenPath {
		t.Errorf("default token path = %q, want %q", c.tokenPath, DefaultTokenPath)
	}
}
