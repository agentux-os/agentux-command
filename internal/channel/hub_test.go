package channel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// node connects to the hub and answers every command with its method name,
// counting the commands it got.
func node(t *testing.T, url string, got *atomic.Int32) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := ws.Write(ctx, websocket.MessageText, []byte(`{"jsonrpc":"2.0","method":"node.heartbeat","params":{}}`)); err != nil {
		t.Fatalf("write: %v", err)
	}
	go func() {
		for {
			_, data, err := ws.Read(context.Background())
			if err != nil {
				return
			}
			var request struct {
				ID     string `json:"id"`
				Method string `json:"method"`
			}
			_ = json.Unmarshal(data, &request)
			got.Add(1)
			answer, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]string{"ran": request.Method}})
			_ = ws.Write(context.Background(), websocket.MessageText, answer)
		}
	}()
	return ws
}

func TestCommandsReachTheNodeOnceAndMessagesReachTheCallback(t *testing.T) {
	hub := NewHub()
	var messages atomic.Int32
	hub.OnMessage = func(nodeID string, raw []byte) {
		if nodeID == "node-1" && strings.Contains(string(raw), "node.heartbeat") {
			messages.Add(1)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.Serve(w, r, r.URL.Query().Get("nodeId"))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := hub.Send(ctx, "node-1", "harness.list", map[string]any{"commandId": "c1"}); err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}

	var got atomic.Int32
	ws := node(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/?nodeId=node-1", &got)
	defer ws.CloseNow()
	for !hub.Connected("node-1") {
		time.Sleep(5 * time.Millisecond)
	}

	answer, err := hub.Send(ctx, "node-1", "harness.list", map[string]any{"commandId": "c1"})
	if err != nil || string(answer.Result) != `{"ran":"harness.list"}` {
		t.Fatalf("unexpected answer %s, %v", answer.Result, err)
	}
	// The same commandId again: the first answer, without the node.
	again, err := hub.Send(ctx, "node-1", "harness.list", map[string]any{"commandId": "c1"})
	if err != nil || string(again.Result) != string(answer.Result) || got.Load() != 1 {
		t.Fatalf("expected a replayed answer and one command at the node, got %d", got.Load())
	}
	if _, err := hub.Send(ctx, "node-1", "harness.list", map[string]any{}); err == nil {
		t.Fatal("a command without commandId must be refused")
	}
	for messages.Load() == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("the node's own message never reached OnMessage")
		case <-time.After(5 * time.Millisecond):
		}
	}

	ws.Close(websocket.StatusNormalClosure, "bye")
	for hub.Connected("node-1") {
		time.Sleep(5 * time.Millisecond)
	}
}
