package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/agentux-os/agentux-command/internal/nodes"
	"github.com/agentux-os/agentux-command/internal/office"
	"github.com/agentux-os/agentux-command/internal/protocol"
)

type Handler struct {
	registry *nodes.Registry
	office   *office.Store
}

func NewHandler() *Handler {
	return &Handler{registry: nodes.NewRegistry(), office: office.NewStore()}
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
	case r.Method == http.MethodGet && r.URL.Path == "/v1/office/projection":
		h.projection(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/approvals/") && strings.HasSuffix(r.URL.Path, "/resolve"):
		h.resolveApproval(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) projection(w http.ResponseWriter, r *http.Request) {
	if !authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	if !hasCapability(r, "organization.read") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "organization.read capability required"})
		return
	}
	organizationID := r.URL.Query().Get("organizationId")
	if organizationID == "" {
		organizationID = "org-demo"
	}
	projection, ok := h.office.GetProjection(organizationID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "projection_not_found"})
		return
	}
	writeJSON(w, http.StatusOK, projection)
}

func (h *Handler) resolveApproval(w http.ResponseWriter, r *http.Request) {
	if !authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	if !hasCapability(r, "approval.decide") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "approval.decide capability required"})
		return
	}
	approvalID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/approvals/"), "/resolve")
	var request office.ResolutionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	response, err := h.office.ResolveApproval(approvalID, request)
	if err != nil {
		if conflict, ok := err.(*office.ConflictError); ok {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "conflict": conflict})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func hasCapability(r *http.Request, capability string) bool {
	switch r.Header.Get("Authorization") {
	case "Bearer demo-leader":
		return capability == "organization.read" || capability == "approval.decide"
	case "Bearer demo-observer":
		return capability == "organization.read"
	default:
		return false
	}
}

func authorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer demo-leader" || r.Header.Get("Authorization") == "Bearer demo-observer"
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
