package server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/agentux-os/agentux-command/internal/channel"
	"github.com/agentux-os/agentux-command/internal/enroll"
	"github.com/agentux-os/agentux-command/internal/nodes"
	"github.com/agentux-os/agentux-command/internal/office"
	"github.com/agentux-os/agentux-command/internal/protocol"
)

type Handler struct {
	registry *nodes.Registry
	office   office.OfficeRepository
	hub      *channel.Hub
	// Node id to a static bearer token (AGENTUX_COMMAND_NODE_TOKENS).
	nodeTokens map[string]string
	// Enrolled nodes and this server's instance id.
	enroll   enroll.Store
	instance string
	// Accept protocol messages from nodes nobody enrolled (the demo only).
	openNodes bool
}

// WithEnrollStore keeps enrolled nodes in store (PostgreSQL in production).
func (h *Handler) WithEnrollStore(ctx context.Context, store enroll.Store) (*Handler, error) {
	instance, err := store.InstanceID(ctx)
	if err != nil {
		return nil, err
	}
	h.enroll, h.instance = store, instance
	return h, nil
}

// WithOpenNodes says whether nodes that are neither enrolled nor listed in
// the static tokens may report (development only).
func (h *Handler) WithOpenNodes(open bool) *Handler {
	h.openNodes = open
	return h
}

// InstanceID is this Command server's identity, which nodes pin.
func (h *Handler) InstanceID() string { return h.instance }

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
	memory := enroll.NewMemory("")
	instance, _ := memory.InstanceID(context.Background())
	h := &Handler{registry: nodes.NewRegistry(), office: repository, hub: channel.NewHub(), enroll: memory, instance: instance, openNodes: true}
	h.hub.OnMessage = func(nodeID string, raw []byte) {
		// A node's own connection only speaks for that node.
		var probe struct {
			Params struct {
				NodeID string `json:"nodeId"`
			} `json:"params"`
		}
		if json.Unmarshal(raw, &probe) == nil && probe.Params.NodeID == nodeID {
			h.apply(raw)
		}
	}
	return h
}

// The commands operators may send to a node (AUXP command → node).
var nodeCommands = map[string]bool{
	"run.start": true, "run.cancel": true, "run.pause": true, "run.resume": true, "run.snapshot": true,
	"request.resolve": true, "bus.post": true,
	"harness.list": true, "harness.test": true, "limits.clear": true, "preferences.set": true,
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
		list := h.registry.List()
		for i := range list {
			list[i].Connected = h.hub.Connected(list[i].ID)
		}
		writeJSON(w, http.StatusOK, list)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/instance":
		writeJSON(w, http.StatusOK, map[string]any{"product": "agentux-command", "instanceId": h.instance, "protocolVersions": []string{"v1"}})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/enroll":
		h.redeem(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/enrollment-tokens":
		h.issueTokens(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/enrollments":
		h.listEnrollments(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/nodes/") && (strings.HasSuffix(r.URL.Path, "/approve") || strings.HasSuffix(r.URL.Path, "/revoke")):
		h.decideNode(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/protocol/connect":
		h.connect(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/nodes/") && strings.HasSuffix(r.URL.Path, "/commands"):
		h.command(w, r)
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
	h, err := NewHandlerWithOfficeRepository(office.NewPostgresStore(db)).WithEnrollStore(ctx, enroll.NewPostgres(db))
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return h.WithOpenNodes(false), db.Close, nil
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
		return capability == "organization.read" || capability == "approval.decide" || capability == "node.command" || capability == "node.enroll"
	case "Bearer demo-observer":
		return capability == "organization.read"
	default:
		return false
	}
}

func authorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer demo-leader" || r.Header.Get("Authorization") == "Bearer demo-observer"
}

// nodeAccess is what a request's node credential allows.
type nodeAccess int

const (
	accessNone    nodeAccess = iota
	accessPending            // enrolled, waiting for an operator's approval
	accessFull
)

// nodeAuthorized checks the node's bearer token: its enrollment credential
// (bound to its organization), a static token, or, on the demo server only,
// none.
func (h *Handler) nodeAuthorized(r *http.Request, nodeID, organizationID string) nodeAccess {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if strings.HasPrefix(got, enroll.SecretPrefix) && h.enroll != nil {
		credential, err := h.enroll.Authenticate(r.Context(), nodeID, got)
		if err != nil || (organizationID != "" && credential.OrganizationID != organizationID) {
			return accessNone
		}
		if !credential.Active() {
			return accessPending
		}
		return accessFull
	}
	if want, known := h.nodeTokens[nodeID]; known {
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1 {
			return accessFull
		}
		return accessNone
	}
	if h.openNodes && len(h.nodeTokens) == 0 {
		return accessFull
	}
	return accessNone
}

func (h *Handler) message(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cannot read the message"})
		return
	}
	var message protocol.Message
	if json.Unmarshal(body, &message) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if err := protocol.Validate(message); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	access := h.nodeAuthorized(r, message.Params.NodeID, message.Params.OrganizationID)
	if access == accessNone {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unknown node or token"})
		return
	}
	if access == accessPending {
		// Until an operator approves it, a node may only say it exists.
		if message.Method != "node.hello" && message.Method != "node.heartbeat" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "the node waits for an operator's approval", "instanceId": h.instance})
			return
		}
		h.apply(body)
		h.registry.HeartbeatWith(message.Params.NodeID, "pending_approval", nil, message.Params.OccurredAt)
	} else {
		h.apply(body)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": map[string]string{"accepted": "true", "instanceId": h.instance}})
}

// apply records a valid node message in the registry.
func (h *Handler) apply(body []byte) {
	var message protocol.Message
	var raw struct {
		Params json.RawMessage `json:"params"`
	}
	if json.Unmarshal(body, &message) != nil || json.Unmarshal(body, &raw) != nil || protocol.Validate(message) != nil {
		return
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
}

// connect accepts a node's command connection.
func (h *Handler) connect(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("nodeId")
	if nodeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "nodeId is required"})
		return
	}
	switch h.nodeAuthorized(r, nodeID, "") {
	case accessNone:
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unknown node or token"})
		return
	case accessPending:
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "the node waits for an operator's approval"})
		return
	}
	// Nodes check they reached the Command server they enrolled with.
	w.Header().Set("X-AgentUX-Command-Instance", h.instance)
	h.hub.Serve(w, r, nodeID)
}

// command sends an operator's command to a node and returns its answer.
func (h *Handler) command(w http.ResponseWriter, r *http.Request) {
	if !authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	if !hasCapability(r, "node.command") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "node.command capability required"})
		return
	}
	nodeID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/nodes/"), "/commands")
	node, known := h.registry.Get(nodeID)
	if !known {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown node"})
		return
	}
	var request struct {
		Method    string         `json:"method"`
		CommandID string         `json:"commandId"`
		Params    map[string]any `json:"params"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request) != nil || !nodeCommands[request.Method] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a known node command method and JSON params are required"})
		return
	}
	for _, key := range []string{"apiKey", "providerToken"} {
		if _, ok := request.Params[key]; ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provider credentials are not protocol payloads"})
			return
		}
	}
	if request.CommandID == "" {
		request.CommandID = newID()
	}
	params := map[string]any{}
	for k, v := range request.Params {
		params[k] = v
	}
	params["protocolVersion"] = "v1"
	params["organizationId"] = node.OrganizationID
	params["nodeId"] = nodeID
	params["messageId"] = request.CommandID
	params["commandId"] = request.CommandID
	params["occurredAt"] = time.Now().UTC().Format(time.RFC3339)
	params["commandInstanceId"] = h.instance

	ctx, cancel := context.WithTimeout(r.Context(), 150*time.Second)
	defer cancel()
	answer, err := h.hub.Send(ctx, nodeID, request.Method, params)
	switch {
	case errors.Is(err, channel.ErrNotConnected):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "the node is not connected", "commandId": request.CommandID})
	case err != nil:
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": err.Error(), "commandId": request.CommandID})
	case answer.Error != nil:
		status := http.StatusBadGateway
		if answer.Error.Code == -32010 || answer.Error.Code == -32011 {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]any{"commandId": request.CommandID, "error": answer.Error})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"commandId": request.CommandID, "result": answer.Result})
	}
}

// redeem trades an enrollment token for the node's credential.
func (h *Handler) redeem(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Token    string `json:"token"`
		NodeName string `json:"nodeName"`
		Hostname string `json:"hostname"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&request) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	redemption, err := h.enroll.Redeem(r.Context(), request.Token, request.NodeName, request.Hostname, time.Now())
	switch {
	case errors.Is(err, enroll.ErrOtherInstance):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error(), "instanceId": h.instance})
		return
	case errors.Is(err, enroll.ErrInvalidToken), errors.Is(err, enroll.ErrUsedToken), errors.Is(err, enroll.ErrExpiredToken):
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	case err != nil:
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"instanceId":     h.instance,
		"organizationId": redemption.Credential.OrganizationID,
		"nodeId":         redemption.Credential.NodeID,
		"nodeSecret":     redemption.Secret,
		"approved":       redemption.Credential.Approved,
	})
}

// issueTokens creates enrollment tokens (operators with node.enroll).
func (h *Handler) issueTokens(w http.ResponseWriter, r *http.Request) {
	if !authorized(r) || !hasCapability(r, "node.enroll") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "node.enroll capability required"})
		return
	}
	var request struct {
		OrganizationID  string `json:"organizationId"`
		NodeName        string `json:"nodeName"`
		Count           int    `json:"count"`
		TTLMinutes      int    `json:"ttlMinutes"`
		RequireApproval bool   `json:"requireApproval"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&request) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	tokens, err := h.enroll.IssueTokens(r.Context(), enroll.TokenRequest{
		OrganizationID: request.OrganizationID, NodeName: request.NodeName, Count: request.Count,
		TTL: time.Duration(request.TTLMinutes) * time.Minute, RequireApproval: request.RequireApproval,
	}, time.Now())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"instanceId": h.instance, "tokens": tokens})
}

// listEnrollments shows every enrolled node and its state.
func (h *Handler) listEnrollments(w http.ResponseWriter, r *http.Request) {
	if !authorized(r) || !hasCapability(r, "organization.read") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "organization.read capability required"})
		return
	}
	credentials, err := h.enroll.Credentials(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "enrollment store unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"instanceId": h.instance, "nodes": credentials})
}

// decideNode approves a node waiting for approval, or revokes a node.
func (h *Handler) decideNode(w http.ResponseWriter, r *http.Request) {
	if !authorized(r) || !hasCapability(r, "node.enroll") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "node.enroll capability required"})
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/nodes/")
	nodeID, action, _ := strings.Cut(path, "/")
	var err error
	if action == "approve" {
		err = h.enroll.Approve(r.Context(), nodeID, time.Now())
	} else {
		err = h.enroll.Revoke(r.Context(), nodeID, time.Now())
		if err == nil {
			h.hub.Disconnect(nodeID)
		}
	}
	if errors.Is(err, enroll.ErrUnknownNode) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown node"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"nodeId": nodeID, "status": action + "d"})
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "cmd-" + hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
