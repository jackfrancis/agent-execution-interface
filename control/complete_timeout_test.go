package control

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// TestCompleteBodyReadTimeoutIs408: an authenticated /complete whose body stalls
// past the server's read timeout gets 408, which the runtime SDK retries, not the
// final 403 a run-id mismatch would produce.
func TestCompleteBodyReadTimeoutIs408(t *testing.T) {
	fp := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	p := New(Config{Secret: []byte("k")})
	srv := httptest.NewUnstartedServer(p.Handler())
	srv.Config.ReadTimeout = 200 * time.Millisecond
	srv.Start()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = p.Run(ctx, fp, aei.RunSpec{ID: "run-slow", Deadline: 5 * time.Second}) }()
	cred := <-fp.dispatched

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = io.WriteString(conn, "POST /aei/v1alpha1/complete HTTP/1.1\r\nHost: aei\r\n"+
		"Authorization: Bearer "+cred.Value+"\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n"+
		`{"run_id":`)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408", resp.StatusCode)
	}
}

// TestCompleteMalformedBodyIs400: a body that is not JSON is a client error, not a
// run-id mismatch.
func TestCompleteMalformedBodyIs400(t *testing.T) {
	fp := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	p := New(Config{Secret: []byte("k")})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = p.Run(ctx, fp, aei.RunSpec{ID: "run-bad", Deadline: 5 * time.Second}) }()
	cred := <-fp.dispatched

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/aei/v1alpha1/complete", strings.NewReader("not json"))
	req.Header.Set("Authorization", "Bearer "+cred.Value)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
