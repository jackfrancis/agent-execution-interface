package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/jackfrancis/agent-execution-interface/control"
	"github.com/jackfrancis/agent-execution-interface/controller/internal/dispatch"
)

type noAuth struct{}

func (noAuth) Authenticate(context.Context, string) (dispatch.Caller, error) {
	return dispatch.Caller{}, errors.New("no")
}

// routeServed reports whether h has a route for method+path: an unrouted request
// gets the mux's own 404 (or 405 for a known path with another method), anything
// else came from a handler.
func routeServed(t *testing.T, base, method, path string) bool {
	t.Helper()
	req, _ := http.NewRequest(method, base+path, strings.NewReader("{}"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed
}

// TestListenersSplitRoutes: the dispatch listener serves only the dispatch API,
// and the ABI listener serves only what runtime pods and Prometheus call, so a
// runtime pod admitted to the ABI port cannot reach dispatch through it.
func TestListenersSplitRoutes(t *testing.T) {
	// An Ed25519 authority, so /authority answers instead of 404ing as it does for
	// the symmetric one.
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	plane := control.New(control.Config{Authority: control.NewEd25519Authority(priv)})
	disp := dispatch.NewHandler(plane, noAuth{}, fake.NewClientBuilder().Build())
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	abiMux, dispatchMux := newMuxes(plane, disp, metrics)
	ctx, cancel := context.WithCancel(context.Background())
	abi, abiDone := startServer(t, ctx, abiMux)
	dsp, dspDone := startServer(t, ctx, dispatchMux)

	abiBase, dspBase := "http://"+abi, "http://"+dsp
	runtimeRoutes := [][2]string{
		{http.MethodPost, "/aei/v1alpha1/complete"},
		{http.MethodPost, "/aei/v1alpha1/redeem"},
		{http.MethodPost, "/aei/v1alpha1/vend"},
		{http.MethodGet, "/aei/v1alpha1/authority"},
		{http.MethodGet, "/metrics"},
	}
	dispatchRoutes := [][2]string{
		{http.MethodPost, "/aei/v1alpha1/dispatch"},
		{http.MethodGet, "/aei/v1alpha1/runs/r1"},
	}
	for _, r := range runtimeRoutes {
		if !routeServed(t, abiBase, r[0], r[1]) {
			t.Errorf("ABI listener does not serve %s %s", r[0], r[1])
		}
		if routeServed(t, dspBase, r[0], r[1]) {
			t.Errorf("dispatch listener serves %s %s", r[0], r[1])
		}
	}
	for _, r := range dispatchRoutes {
		if !routeServed(t, dspBase, r[0], r[1]) {
			t.Errorf("dispatch listener does not serve %s %s", r[0], r[1])
		}
		if routeServed(t, abiBase, r[0], r[1]) {
			t.Errorf("ABI listener serves %s %s", r[0], r[1])
		}
	}

	cancel()
	for _, done := range []<-chan error{abiDone, dspDone} {
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	}
}

// TestResolveDispatchAddr: without -dispatch-addr, dispatch keeps the ABI
// listener's bind host on port 8081. The two listeners never share a port, even
// on different hosts, since a NetworkPolicy admits by port.
func TestResolveDispatchAddr(t *testing.T) {
	cases := []struct {
		abi, dispatch string
		want          string
		wantErr       bool
	}{
		{abi: ":8080", want: ":8081"},
		{abi: "127.0.0.1:8080", want: "127.0.0.1:8081"},
		{abi: "[::1]:8080", want: "[::1]:8081"},
		{abi: "127.0.0.1:8080", dispatch: "0.0.0.0:9000", want: "0.0.0.0:9000"},
		{abi: ":8081", dispatch: ":9000", want: ":9000"},
		{abi: "10.0.0.5:8080", dispatch: "[fd00::5]:8081", want: "[fd00::5]:8081"},
		{abi: ":8081", wantErr: true},
		{abi: "127.0.0.1:8081", wantErr: true},
		{abi: ":8080", dispatch: ":8080", wantErr: true},
		{abi: "10.0.0.5:8080", dispatch: "[fd00::5]:8080", wantErr: true}, // dual-stack pod, same port
		{abi: "127.0.0.1:8080", dispatch: "0.0.0.0:8080", wantErr: true},
		{abi: ":http", want: ":8081"},
		{abi: ":http", dispatch: ":80", wantErr: true}, // a service name resolves to its number
		{abi: ":8080", dispatch: "no-port", wantErr: true},
		{abi: "no-port", wantErr: true},
	}
	for _, tc := range cases {
		got, err := resolveDispatchAddr(tc.abi, tc.dispatch)
		if tc.wantErr {
			if err == nil {
				t.Errorf("resolveDispatchAddr(%q, %q) = %q, want an error", tc.abi, tc.dispatch, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("resolveDispatchAddr(%q, %q) = %q, %v; want %q", tc.abi, tc.dispatch, got, err, tc.want)
		}
	}
}
