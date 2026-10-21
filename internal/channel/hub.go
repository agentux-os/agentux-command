// Package channel keeps the command connections AgentUX nodes open to
// Command (AUXP over WebSocket, opened by the node) and sends them commands.
//
// A node connects to /v1/protocol/connect. Command writes JSON-RPC requests
// to it; the node answers each with a response carrying the same id (the
// commandId). Anything else the node writes (events, heartbeats) goes to the
// hub's message callback, as if it had been POSTed.
package channel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ErrNotConnected means the node has no command connection open.
var ErrNotConnected = errors.New("the node is not connected")

// Response is a node's answer to one command.
type Response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type conn struct {
	ws      *websocket.Conn
	write   sync.Mutex
	mu      sync.Mutex
	waiting map[string]chan Response
}

// Hub is the set of connected nodes.
type Hub struct {
	mu    sync.Mutex
	conns map[string]*conn
	// Answers by node and commandId: a command sent again gets the first
	// answer without reaching the node twice.
	answers map[string]map[string]Response
	// Called with each message a node sends that is not a response.
	OnMessage func(nodeID string, raw []byte)
}

func NewHub() *Hub {
	return &Hub{conns: make(map[string]*conn), answers: make(map[string]map[string]Response)}
}

// Connected reports whether a node has a command connection.
func (h *Hub) Connected(nodeID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.conns[nodeID]
	return ok
}

// Serve accepts a node's connection and reads from it until it closes. A
// node connecting again replaces its previous connection.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, nodeID string) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ws.SetReadLimit(4 << 20)
	c := &conn{ws: ws, waiting: make(map[string]chan Response)}
	h.mu.Lock()
	if previous, ok := h.conns[nodeID]; ok {
		previous.ws.Close(websocket.StatusPolicyViolation, "replaced by a new connection")
	}
	h.conns[nodeID] = c
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		if h.conns[nodeID] == c {
			delete(h.conns, nodeID)
		}
		h.mu.Unlock()
		c.mu.Lock()
		for id, ch := range c.waiting {
			close(ch)
			delete(c.waiting, id)
		}
		c.mu.Unlock()
		ws.CloseNow()
	}()

	ctx := r.Context()
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var envelope struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Result json.RawMessage `json:"result"`
			Error  *RPCError       `json:"error"`
		}
		if json.Unmarshal(data, &envelope) != nil {
			continue
		}
		if envelope.Method == "" && len(envelope.ID) > 0 {
			var id string
			if json.Unmarshal(envelope.ID, &id) != nil {
				continue
			}
			c.mu.Lock()
			ch, ok := c.waiting[id]
			delete(c.waiting, id)
			c.mu.Unlock()
			if ok {
				ch <- Response{Result: envelope.Result, Error: envelope.Error}
			}
			continue
		}
		if h.OnMessage != nil {
			h.OnMessage(nodeID, data)
		}
	}
}

// Send writes a command to a node and waits for its answer, at most until
// ctx ends. params must carry the commandId, which is also the request id.
func (h *Hub) Send(ctx context.Context, nodeID, method string, params map[string]any) (Response, error) {
	commandID, _ := params["commandId"].(string)
	if commandID == "" {
		return Response{}, fmt.Errorf("a command needs a commandId")
	}
	h.mu.Lock()
	if answer, ok := h.answers[nodeID][commandID]; ok {
		h.mu.Unlock()
		return answer, nil
	}
	c, ok := h.conns[nodeID]
	h.mu.Unlock()
	if !ok {
		return Response{}, ErrNotConnected
	}

	ch := make(chan Response, 1)
	c.mu.Lock()
	c.waiting[commandID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.waiting, commandID)
		c.mu.Unlock()
	}()

	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": commandID, "method": method, "params": params})
	if err != nil {
		return Response{}, err
	}
	c.write.Lock()
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = c.ws.Write(writeCtx, websocket.MessageText, request)
	cancel()
	c.write.Unlock()
	if err != nil {
		return Response{}, fmt.Errorf("cannot write to the node: %w", err)
	}
	select {
	case answer, ok := <-ch:
		if !ok {
			return Response{}, fmt.Errorf("the node disconnected before answering")
		}
		h.mu.Lock()
		if h.answers[nodeID] == nil {
			h.answers[nodeID] = make(map[string]Response)
		}
		if len(h.answers[nodeID]) > 10000 {
			h.answers[nodeID] = make(map[string]Response)
		}
		h.answers[nodeID][commandID] = answer
		h.mu.Unlock()
		return answer, nil
	case <-ctx.Done():
		return Response{}, fmt.Errorf("the node did not answer in time: %w", ctx.Err())
	}
}
