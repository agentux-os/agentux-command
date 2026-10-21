package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/agentux-os/agentux-command/internal/nodes"
	"github.com/agentux-os/agentux-command/internal/office"
	"github.com/agentux-os/agentux-command/internal/protocol"
)

type Handler struct {
	registry *nodes.Registry
	office   office.OfficeRepository
	// Node id to its bearer token. Empty: protocol messages are not
	// authenticated (development only).
	nodeTokens map[string]string
}

// WithNodeTokens requires each node's protocol messages to carry its token
// (`Authorization: Bearer <token>`).
func (h *Handler) WithNodeTokens(tokens map[string]string) *Handler {
	h.nodeTokens = tokens
	return h
}

// ParseNodeTokens reads `node-1=token1,node-2=token2`.
func ParseNodeTokens(text string) map[string]string {
	tokens := make(map[string]string)
	for _, entry := range strings.Split(text, ",") {
		node, token, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if ok && node != "" && token != "" {
			tokens[strings.TrimSpace(node)] = strings.TrimSpace(token)
		}
	}
	return tokens
}

func NewHandler() *Handler {
	return NewHandlerWithOfficeRepository(office.NewStore())
}

func NewHandlerWithOfficeRepository(repository office.OfficeRepository) *Handler {
	if repository == nil {
		repository = office.NewStore()
	}
	return &Handler{registry: nodes.NewRegistry(), office: repository}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		if err := h.office.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
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

func NewPostgresHandler(ctx context.Context, databaseURL string) (*Handler, func() error, error) {
	db, err := office.OpenPostgres(ctx, databaseURL)
	if err != nil {
		return nil, nil, err
	}
	return NewHandlerWithOfficeRepository(office.NewPostgresStore(db)), db.Close, nil
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
	projection, ok, err := h.office.GetProjection(r.Context(), organizationID)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "projection_unavailable"})
		return
	}
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
	response, err := h.office.ResolveApproval(r.Context(), approvalID, request)
	if err != nil {
		if conflict, ok := err.(*office.ConflictError); ok {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "conflict": conflict})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "approval_store_unavailable"})
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cannot read the message"})
		return
	}
	var message protocol.Message
	var raw struct {
		Params json.RawMessage `json:"params"`
	}
	if json.Unmarshal(body, &message) != nil || json.Unmarshal(body, &raw) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if err := protocol.Validate(message); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(h.nodeTokens) > 0 {
		want, known := h.nodeTokens[message.Params.NodeID]
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !known || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unknown node or token"})
			return
		}
	}
	switch message.Method {
	case "node.hello":
		h.registry.Hello(nodes.Node{
			ID: message.Params.NodeID, OrganizationID: message.Params.OrganizationID, Capabilities: message.Params.Capabilities,
			AgentUXVersion: message.Params.AgentUXVersion, LastSeenAt: message.Params.OccurredAt,
		})
	case "node.heartbeat":
		h.registry.HeartbeatWith(message.Params.NodeID, message.Params.Status, message.Params.Capacity, message.Params.OccurredAt)
	default:
		if protocol.EventMethods[message.Method] {
			h.registry.Apply(nodes.Event{
				NodeID: message.Params.NodeID, OrganizationID: message.Params.OrganizationID, Method: message.Method,
				Sequence: message.Params.Sequence, OccurredAt: message.Params.OccurredAt, RunID: message.Params.RunID,
				RequestID: message.Params.RequestID, State: message.Params.State,
				Harnesses: message.Params.Harnesses, Limits: message.Params.Limits, Params: raw.Params,
			})
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": map[string]string{"accepted": "true"}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
