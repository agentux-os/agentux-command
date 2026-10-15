package server

import (
	"encoding/json"
	"net/http"

	"github.com/agentux-os/agentux-command/internal/nodes"
	"github.com/agentux-os/agentux-command/internal/protocol"
)

type Handler struct {
	registry *nodes.Registry
}

func NewHandler() *Handler {
	return &Handler{registry: nodes.NewRegistry()}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/nodes":
		writeJSON(w, http.StatusOK, h.registry.List())
	case r.Method == http.MethodPost && r.URL.Path == "/v1/protocol/messages":
		h.message(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) message(w http.ResponseWriter, r *http.Request) {
	var message protocol.Message
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&message); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if err := protocol.Validate(message); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	switch message.Method {
	case "node.hello":
		h.registry.Hello(nodes.Node{ID: message.Params.NodeID, OrganizationID: message.Params.OrganizationID, Capabilities: message.Params.Capabilities})
	case "node.heartbeat":
		h.registry.Heartbeat(message.Params.NodeID, message.Params.Status)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": map[string]string{"accepted": "true"}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
