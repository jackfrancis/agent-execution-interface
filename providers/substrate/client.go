package substrate

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// client is a minimal A2A JSON-RPC client that reaches a durable Agent Substrate
// actor through the atenet-router. It dispatches one agent task (message/send) and
// reads the terminal task back. routerURL is the in-cluster router root (e.g.
// "http://atenet-router.ate-system.svc.cluster.local"); the actor is addressed by
// the request Host header, which the router matches to resume and proxy to the
// actor's port 80 — so the request path is just "/" (the runtime's JSON-RPC
// endpoint) and the routing key rides the Host, not the path.
type client struct {
	routerURL string
	http      *http.Client
}

func newClient(routerURL string, hc *http.Client) *client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &client{routerURL: strings.TrimRight(routerURL, "/"), http: hc}
}

// rpcRequest is a JSON-RPC 2.0 A2A message/send call.
type rpcRequest struct {
	JSONRPC string     `json:"jsonrpc"`
	ID      string     `json:"id"`
	Method  string     `json:"method"`
	Params  sendParams `json:"params"`
}

type sendParams struct {
	Message       a2aMessage `json:"message"`
	Configuration sendConfig `json:"configuration"`
}

// sendConfig sets A2A message-send options. Blocking is false so the actor
// acknowledges as soon as it accepts the task, instead of holding the connection
// until the job finishes: the runtime runs the job detached and reports completion
// out-of-band (the callback the control plane races against Await).
type sendConfig struct {
	Blocking bool `json:"blocking"`
}

type a2aMessage struct {
	Role      string            `json:"role"`
	Kind      string            `json:"kind"`
	MessageID string            `json:"messageId"`
	Parts     []textPart        `json:"parts"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type textPart struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// taskResult is the subset of a terminal A2A task this launcher acts on.
type taskResult struct {
	TaskID  string
	State   string
	Message string
}

// sendTask posts a non-blocking message/send to the actor addressed by actorHost
// and returns the actor's terminal task. The call is bounded by ctx (the run
// deadline), so the client sets no timeout of its own. The request goes to the
// router root ("/"); actorHost is set as the HTTP Host header (Go still dials
// routerURL), which is the router's routing key: it matches the actor, resumes it
// if suspended, and proxies to it.
func (c *client) sendTask(ctx context.Context, actorHost, prompt string, metadata map[string]string) (taskResult, error) {
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      randID(),
		Method:  "message/send",
		Params: sendParams{
			Message: a2aMessage{
				Role: "user", Kind: "message", MessageID: randID(),
				Parts:    []textPart{{Kind: "text", Text: prompt}},
				Metadata: metadata,
			},
			Configuration: sendConfig{Blocking: false},
		},
	})
	if err != nil {
		return taskResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.routerURL+"/", bytes.NewReader(body))
	if err != nil {
		return taskResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	// The router routes by Host, not by path: set it so the request reaches the
	// actor (and auto-resumes it). Go dials req.URL (the router) but sends this Host.
	req.Host = actorHost

	resp, err := c.http.Do(req)
	if err != nil {
		return taskResult{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return taskResult{}, fmt.Errorf("status %d: %s", resp.StatusCode, snippet(data))
	}

	var rpcResp struct {
		Result *struct {
			ID     string `json:"id"`
			Status struct {
				State   string `json:"state"`
				Message struct {
					Parts []textPart `json:"parts"`
				} `json:"message"`
			} `json:"status"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &rpcResp); err != nil {
		return taskResult{}, fmt.Errorf("decode response: %w", err)
	}
	if rpcResp.Error != nil {
		return taskResult{}, fmt.Errorf("a2a error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	if rpcResp.Result == nil {
		return taskResult{}, fmt.Errorf("response had neither result nor error")
	}
	res := taskResult{TaskID: rpcResp.Result.ID, State: rpcResp.Result.Status.State}
	if len(rpcResp.Result.Status.Message.Parts) > 0 {
		res.Message = rpcResp.Result.Status.Message.Parts[0].Text
	}
	return res, nil
}

func snippet(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}

func randID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "id"
	}
	return hex.EncodeToString(b[:])
}
