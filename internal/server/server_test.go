package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGatewayAcceptsHelloAndListsNode(t *testing.T) {
	handler := NewHandler()
	message := map[string]any{
		"jsonrpc": "2.0", "id": "hello-1", "method": "node.hello",
		"params": map[string]any{
			"protocolVersion": "v1", "organizationId": "org-1", "nodeId": "node-1", "messageId": "msg-1", "occurredAt": "2026-10-15T12:00:00Z",
			"capabilities": []string{"run.start"}, "agentuxVersion": "0.5.0", "supportedProtocolVersions": []string{"v1"},
		},
	}
	body, _ := json.Marshal(message)
	request := httptest.NewRequest(http.MethodPost, "/v1/protocol/messages", bytes.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/v1/nodes", nil))
	if list.Code != http.StatusOK || !bytes.Contains(list.Body.Bytes(), []byte(`"nodeId":"node-1"`)) {
		t.Fatalf("expected node in list, got %d: %s", list.Code, list.Body.String())
	}
}
