package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/agentux-os/agentux-command/internal/office"
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

func TestOfficeProjectionRequiresCapabilityAndReturnsReplayMetadata(t *testing.T) {
	handler := NewHandler()
	request := httptest.NewRequest(http.MethodGet, "/v1/office/projection?organizationId=org-demo", nil)
	request.Header.Set("Authorization", "Bearer demo-leader")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var projection map[string]any
	if err := json.NewDecoder(response.Body).Decode(&projection); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"organizationId", "definitionVersion", "projectedAt", "freshness", "lastEventId", "events"} {
		if projection[field] == nil {
			t.Fatalf("projection missing %s: %#v", field, projection)
		}
	}
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/office/projection", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", unauthorized.Code)
	}
	missing := httptest.NewRecorder()
	missingRequest := httptest.NewRequest(http.MethodGet, "/v1/office/projection?organizationId=org-missing", nil)
	missingRequest.Header.Set("Authorization", "Bearer demo-leader")
	handler.ServeHTTP(missing, missingRequest)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown organization, got %d", missing.Code)
	}
	invalid := httptest.NewRecorder()
	invalidRequest := httptest.NewRequest(http.MethodGet, "/v1/office/projection", nil)
	invalidRequest.Header.Set("Authorization", "Bearer invalid")
	handler.ServeHTTP(invalid, invalidRequest)
	if invalid.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid token, got %d", invalid.Code)
	}
}

func TestOfficeApprovalResolutionIsIdempotent(t *testing.T) {
	handler := NewHandler()
	body := bytes.NewBufferString(`{"organizationId":"org-demo","decision":"approved","reason":"release reviewed","idempotencyKey":"idem-01"}`)
	first := httptest.NewRequest(http.MethodPost, "/v1/approvals/approval-01/resolve", body)
	first.Header.Set("Authorization", "Bearer demo-leader")
	firstResponse := httptest.NewRecorder()
	handler.ServeHTTP(firstResponse, first)
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", firstResponse.Code, firstResponse.Body.String())
	}

	replay := httptest.NewRequest(http.MethodPost, "/v1/approvals/approval-01/resolve", bytes.NewBufferString(`{"organizationId":"org-demo","decision":"approved","reason":"release reviewed","idempotencyKey":"idem-01"}`))
	replay.Header.Set("Authorization", "Bearer demo-leader")
	replayResponse := httptest.NewRecorder()
	handler.ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusOK || !bytes.Contains(replayResponse.Body.Bytes(), []byte(`"idempotentReplay":true`)) {
		t.Fatalf("expected idempotent replay, got %d: %s", replayResponse.Code, replayResponse.Body.String())
	}
	decided := httptest.NewRecorder()
	decidedRequest := httptest.NewRequest(http.MethodPost, "/v1/approvals/approval-01/resolve", bytes.NewBufferString(`{"organizationId":"org-demo","decision":"rejected","reason":"changed","idempotencyKey":"idem-02"}`))
	decidedRequest.Header.Set("Authorization", "Bearer demo-leader")
	handler.ServeHTTP(decided, decidedRequest)
	if decided.Code != http.StatusConflict || !bytes.Contains(decided.Body.Bytes(), []byte(`"already_decided"`)) {
		t.Fatalf("expected already-decided conflict, got %d: %s", decided.Code, decided.Body.String())
	}
	forbidden := httptest.NewRecorder()
	forbiddenRequest := httptest.NewRequest(http.MethodPost, "/v1/approvals/approval-01/resolve", bytes.NewBufferString(`{"organizationId":"org-demo","decision":"approved","idempotencyKey":"idem-03"}`))
	forbiddenRequest.Header.Set("Authorization", "Bearer demo-observer")
	handler.ServeHTTP(forbidden, forbiddenRequest)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for missing capability, got %d", forbidden.Code)
	}
}

type failingOfficeRepository struct{}

func (failingOfficeRepository) GetProjection(context.Context, string) (office.Projection, bool, error) {
	return office.Projection{}, false, errors.New("database unavailable")
}
func (failingOfficeRepository) ApplyEvent(context.Context, office.Event) error {
	return errors.New("database unavailable")
}
func (failingOfficeRepository) Ping(context.Context) error {
	return errors.New("database unavailable")
}
func (failingOfficeRepository) ResolveApproval(context.Context, string, office.ResolutionRequest) (office.ResolutionResponse, error) {
	return office.ResolutionResponse{}, errors.New("database unavailable")
}

func TestOfficeProjectionReportsStorageFailureAsUnavailable(t *testing.T) {
	handler := NewHandlerWithOfficeRepository(failingOfficeRepository{})
	request := httptest.NewRequest(http.MethodGet, "/v1/office/projection?organizationId=org-demo", nil)
	request.Header.Set("Authorization", "Bearer demo-leader")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on storage failure, got %d: %s", response.Code, response.Body.String())
	}
}

func TestHealthCheckReportsDatabaseFailure(t *testing.T) {
	handler := NewHandlerWithOfficeRepository(failingOfficeRepository{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when database is unavailable, got %d", response.Code)
	}
}

func TestProductionHandlerRequiresDatabaseURL(t *testing.T) {
	handler, closeStore, err := NewPostgresHandler(context.Background(), "")
	if err == nil || handler != nil || closeStore != nil {
		t.Fatalf("expected production handler construction to fail without database URL, handler=%v close=%v err=%v", handler, closeStore != nil, err)
	}
}

func post(t *testing.T, handler http.Handler, token string, message map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(message)
	request := httptest.NewRequest(http.MethodPost, "/v1/protocol/messages", bytes.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func nodeParams(extra map[string]any) map[string]any {
	params := map[string]any{"protocolVersion": "v1", "organizationId": "org-1", "nodeId": "node-1", "messageId": "m", "occurredAt": "2026-10-21T12:00:00Z"}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

func TestNodeEventsShowInTheNodeList(t *testing.T) {
	handler := NewHandler().WithNodeTokens(ParseNodeTokens("node-1=s3cret, node-2=other"))
	hello := map[string]any{"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": nodeParams(map[string]any{
		"capabilities": []string{"harness.status"}, "agentuxVersion": "0.4.8", "supportedProtocolVersions": []string{"v1"},
	})}
	if response := post(t, handler, "wrong", hello); response.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a wrong token, got %d", response.Code)
	}
	if response := post(t, handler, "s3cret", hello); response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	status := map[string]any{"jsonrpc": "2.0", "method": "harness.status", "params": nodeParams(map[string]any{
		"eventId": "node-1-1", "sequence": 1, "harnesses": []map[string]any{{"id": "codex", "installed": true, "signedIn": true}},
	})}
	if response := post(t, handler, "s3cret", status); response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	limits := map[string]any{"jsonrpc": "2.0", "method": "harness.limits_changed", "params": nodeParams(map[string]any{
		"eventId": "node-1-2", "sequence": 2, "limits": []map[string]any{{"harness": "codex", "kind": "weekly"}},
	})}
	post(t, handler, "s3cret", limits)

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/v1/nodes", nil))
	var nodesList []map[string]any
	if err := json.Unmarshal(list.Body.Bytes(), &nodesList); err != nil || len(nodesList) != 1 {
		t.Fatalf("unexpected node list: %s", list.Body.String())
	}
	node := nodesList[0]
	if node["agentuxVersion"] != "0.4.8" || node["lastSequence"].(float64) != 2 {
		t.Fatalf("unexpected node: %v", node)
	}
	if !bytes.Contains(list.Body.Bytes(), []byte(`"kind":"weekly"`)) || !bytes.Contains(list.Body.Bytes(), []byte(`"signedIn":true`)) {
		t.Fatalf("expected harnesses and limits in the list: %s", list.Body.String())
	}
}

func TestOperatorsSendCommandsToConnectedNodes(t *testing.T) {
	handler := NewHandler().WithNodeTokens(ParseNodeTokens("node-1=s3cret"))
	server := httptest.NewServer(handler)
	defer server.Close()
	hello := map[string]any{"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": nodeParams(map[string]any{
		"capabilities": []string{"harness.list"}, "agentuxVersion": "0.4.9", "supportedProtocolVersions": []string{"v1"},
	})}
	post(t, handler, "s3cret", hello)

	command := func(token string, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1/nodes/node-1/commands", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := command("demo-observer", `{"method":"harness.list","params":{}}`); response.Code != http.StatusForbidden {
		t.Fatalf("observers must not send commands, got %d", response.Code)
	}
	if response := command("demo-leader", `{"method":"harness.list","params":{}}`); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before the node connects, got %d", response.Code)
	}
	if response := command("demo-leader", `{"method":"shell.exec","params":{}}`); response.Code != http.StatusBadRequest {
		t.Fatalf("unknown methods must be refused, got %d", response.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/protocol/connect?nodeId=node-1"
	if _, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer wrong"}}}); err == nil {
		t.Fatal("a wrong node token must not connect")
	}
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer s3cret"}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.CloseNow()
	go func() {
		for {
			_, data, err := ws.Read(context.Background())
			if err != nil {
				return
			}
			var request struct {
				ID     string         `json:"id"`
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
			}
			_ = json.Unmarshal(data, &request)
			answer := map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"method": request.Method, "nodeId": request.Params["nodeId"]}}
			if request.Method == "preferences.set" {
				answer = map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32010, "message": "awaiting_local_confirmation"}}
			}
			out, _ := json.Marshal(answer)
			_ = ws.Write(context.Background(), websocket.MessageText, out)
		}
	}()
	for !handler.hub.Connected("node-1") {
		time.Sleep(5 * time.Millisecond)
	}

	response := command("demo-leader", `{"method":"harness.list","commandId":"cmd-1","params":{}}`)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"nodeId":"node-1"`) {
		t.Fatalf("expected the node's answer, got %d: %s", response.Code, response.Body.String())
	}
	response = command("demo-leader", `{"method":"preferences.set","params":{"preferences":{}}}`)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "awaiting_local_confirmation") {
		t.Fatalf("expected a held command, got %d: %s", response.Code, response.Body.String())
	}

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/v1/nodes", nil))
	if !strings.Contains(list.Body.String(), `"connected":true`) {
		t.Fatalf("expected the node connected: %s", list.Body.String())
	}
}

func call(t *testing.T, handler http.Handler, method, path, token string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	data, _ := json.Marshal(body)
	request := httptest.NewRequest(method, path, bytes.NewReader(data))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var out map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &out)
	return response, out
}

func TestNodesEnrollWithOneCommandOnly(t *testing.T) {
	ours := NewHandler().WithOpenNodes(false)
	theirs := NewHandler().WithOpenNodes(false)
	if ours.InstanceID() == theirs.InstanceID() {
		t.Fatal("each Command server has its own instance id")
	}
	_, instance := call(t, ours, http.MethodGet, "/v1/instance", "", nil)
	if instance["instanceId"] != ours.InstanceID() {
		t.Fatalf("unexpected instance: %v", instance)
	}

	if response, _ := call(t, ours, http.MethodPost, "/v1/enrollment-tokens", "demo-observer", map[string]any{"organizationId": "acme"}); response.Code != http.StatusForbidden {
		t.Fatalf("observers must not issue tokens, got %d", response.Code)
	}
	_, issued := call(t, ours, http.MethodPost, "/v1/enrollment-tokens", "demo-leader", map[string]any{"organizationId": "acme", "count": 2})
	tokens := issued["tokens"].([]any)
	token := tokens[0].(map[string]any)["token"].(string)
	_, foreign := call(t, theirs, http.MethodPost, "/v1/enrollment-tokens", "demo-leader", map[string]any{"organizationId": "acme"})
	foreignToken := foreign["tokens"].([]any)[0].(map[string]any)["token"].(string)

	// Another Command's token cannot capture this node, nor ours theirs.
	if response, _ := call(t, ours, http.MethodPost, "/v1/enroll", "", map[string]any{"token": foreignToken, "hostname": "box"}); response.Code != http.StatusConflict {
		t.Fatalf("expected 409 for another instance's token, got %d", response.Code)
	}
	response, enrolled := call(t, ours, http.MethodPost, "/v1/enroll", "", map[string]any{"token": token, "hostname": "box"})
	if response.Code != http.StatusOK || enrolled["instanceId"] != ours.InstanceID() || enrolled["organizationId"] != "acme" {
		t.Fatalf("enroll: %d %v", response.Code, enrolled)
	}
	nodeID, secret := enrolled["nodeId"].(string), enrolled["nodeSecret"].(string)
	if response, _ := call(t, ours, http.MethodPost, "/v1/enroll", "", map[string]any{"token": token}); response.Code != http.StatusUnauthorized {
		t.Fatalf("a token is used once, got %d", response.Code)
	}

	params := func(org string, extra map[string]any) map[string]any {
		p := map[string]any{"protocolVersion": "v1", "organizationId": org, "nodeId": nodeID, "messageId": "m", "occurredAt": "2026-10-21T12:00:00Z"}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}
	hello := map[string]any{"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": params("acme", map[string]any{
		"capabilities": []string{"harness.list"}, "agentuxVersion": "0.4.9", "supportedProtocolVersions": []string{"v1"},
	})}
	response, accepted := call(t, ours, http.MethodPost, "/v1/protocol/messages", secret, hello)
	if response.Code != http.StatusAccepted || accepted["result"].(map[string]any)["instanceId"] != ours.InstanceID() {
		t.Fatalf("hello: %d %v", response.Code, accepted)
	}
	// The credential is bound to its organization and to this server.
	wrongOrg := map[string]any{"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": params("other-org", map[string]any{
		"capabilities": []string{"harness.list"}, "agentuxVersion": "0.4.9", "supportedProtocolVersions": []string{"v1"},
	})}
	if response, _ := call(t, ours, http.MethodPost, "/v1/protocol/messages", secret, wrongOrg); response.Code != http.StatusUnauthorized {
		t.Fatalf("another organization must be refused, got %d", response.Code)
	}
	if response, _ := call(t, theirs, http.MethodPost, "/v1/protocol/messages", secret, hello); response.Code != http.StatusUnauthorized {
		t.Fatalf("another Command must refuse the node, got %d", response.Code)
	}

	// Revoked: refused.
	if response, _ := call(t, ours, http.MethodPost, "/v1/nodes/"+nodeID+"/revoke", "demo-leader", nil); response.Code != http.StatusOK {
		t.Fatalf("revoke: %d", response.Code)
	}
	if response, _ := call(t, ours, http.MethodPost, "/v1/protocol/messages", secret, hello); response.Code != http.StatusUnauthorized {
		t.Fatalf("a revoked node must be refused, got %d", response.Code)
	}
}

func TestNodesWaitingForApprovalOnlySayHello(t *testing.T) {
	handler := NewHandler().WithOpenNodes(false)
	_, issued := call(t, handler, http.MethodPost, "/v1/enrollment-tokens", "demo-leader", map[string]any{"organizationId": "acme", "nodeName": "gpu-1", "requireApproval": true})
	token := issued["tokens"].([]any)[0].(map[string]any)["token"].(string)
	_, enrolled := call(t, handler, http.MethodPost, "/v1/enroll", "", map[string]any{"token": token})
	if enrolled["nodeId"] != "gpu-1" || enrolled["approved"] != false {
		t.Fatalf("unexpected enrollment: %v", enrolled)
	}
	secret := enrolled["nodeSecret"].(string)
	base := map[string]any{"protocolVersion": "v1", "organizationId": "acme", "nodeId": "gpu-1", "messageId": "m", "occurredAt": "2026-10-21T12:00:00Z"}
	event := map[string]any{"jsonrpc": "2.0", "method": "harness.limits_changed", "params": merge(base, map[string]any{"eventId": "e1", "sequence": 1, "limits": []any{}})}
	if response, _ := call(t, handler, http.MethodPost, "/v1/protocol/messages", secret, event); response.Code != http.StatusForbidden {
		t.Fatalf("a node waiting for approval must not send events, got %d", response.Code)
	}
	hello := map[string]any{"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": merge(base, map[string]any{
		"capabilities": []string{"harness.list"}, "agentuxVersion": "0.4.9", "supportedProtocolVersions": []string{"v1"},
	})}
	if response, _ := call(t, handler, http.MethodPost, "/v1/protocol/messages", secret, hello); response.Code != http.StatusAccepted {
		t.Fatalf("hello is accepted while waiting, got %d", response.Code)
	}
	_, listed := call(t, handler, http.MethodGet, "/v1/nodes", "", nil)
	_ = listed
	call(t, handler, http.MethodPost, "/v1/nodes/gpu-1/approve", "demo-leader", nil)
	if response, _ := call(t, handler, http.MethodPost, "/v1/protocol/messages", secret, event); response.Code != http.StatusAccepted {
		t.Fatalf("an approved node sends events, got %d", response.Code)
	}
}

func merge(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func TestTokensComeWithProvisionFiles(t *testing.T) {
	handler := NewHandler().WithPublicURL("https://command.acme.org/")
	response, issued := call(t, handler, http.MethodPost, "/v1/enrollment-tokens", "demo-leader", map[string]any{
		"organizationId": "acme", "count": 2,
		"provision": map[string]any{"profile": "node", "user": "dev", "policy": map[string]any{"commands": map[string]any{"run.start": "allow"}}},
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("issue: %d %v", response.Code, issued)
	}
	tokens := issued["tokens"].([]any)
	file := tokens[1].(map[string]any)["provision"].(map[string]any)
	join := file["command"].(map[string]any)
	if file["profile"] != "node" || file["user"] != "dev" || join["url"] != "https://command.acme.org" || join["enrollmentToken"] != tokens[1].(map[string]any)["token"] {
		t.Fatalf("unexpected provision file: %v", file)
	}
	if response, _ := call(t, handler, http.MethodPost, "/v1/enrollment-tokens", "demo-leader", map[string]any{
		"organizationId": "acme", "provision": map[string]any{"profile": "executive", "user": "dev"},
	}); response.Code != http.StatusBadRequest {
		t.Fatalf("an Executive runs no node to provision, got %d", response.Code)
	}
}
