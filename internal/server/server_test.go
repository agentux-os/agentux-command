package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
