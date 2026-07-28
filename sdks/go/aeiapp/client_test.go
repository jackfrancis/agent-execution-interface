package aeiapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
