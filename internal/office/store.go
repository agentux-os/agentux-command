package office

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type OfficeRepository interface {
	GetProjection(context.Context, string) (Projection, bool, error)
	ApplyEvent(context.Context, Event) error
	ResolveApproval(context.Context, string, ResolutionRequest) (ResolutionResponse, error)
}

type Event struct {
	EventID        string          `json:"eventId"`
	EventType      string          `json:"eventType"`
	OrganizationID string          `json:"organizationId"`
	NodeID         string          `json:"nodeId,omitempty"`
	RunID          string          `json:"runId,omitempty"`
	OccurredAt     string          `json:"occurredAt,omitempty"`
	ReceivedAt     string          `json:"receivedAt,omitempty"`
	Sequence       int64           `json:"sequence"`
	CorrelationID  string          `json:"correlationId,omitempty"`
	CausationID    string          `json:"causationId,omitempty"`
	Data           json.RawMessage `json:"data,omitempty"`
}

type Projection struct {
	OrganizationID    string           `json:"organizationId"`
	SchemaVersion     string           `json:"schemaVersion"`
	DefinitionVersion string           `json:"definitionVersion"`
	ProjectedAt       string           `json:"projectedAt"`
	Source            string           `json:"source"`
	Freshness         string           `json:"freshness"`
	LastEventID       string           `json:"lastEventId"`
	LastNodeID        string           `json:"lastNodeId,omitempty"`
	LastSequence      int64            `json:"lastSequence,omitempty"`
	Cursors           map[string]int64 `json:"cursors"`
	Organization      map[string]any   `json:"organization"`
	Areas             []map[string]any `json:"areas"`
	Missions          []map[string]any `json:"missions"`
	Agents            []map[string]any `json:"agents"`
	Nodes             []map[string]any `json:"nodes"`
	Runs              []map[string]any `json:"runs"`
	Approvals         []map[string]any `json:"approvals"`
	Artifacts         []map[string]any `json:"artifacts"`
	Events            []Event          `json:"events"`
}

type ResolutionRequest struct {
	OrganizationID string `json:"organizationId"`
	Decision       string `json:"decision"`
	Reason         string `json:"reason,omitempty"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type ResolutionResponse struct {
	OK               bool   `json:"ok"`
	OrganizationID   string `json:"organizationId"`
	ApprovalID       string `json:"approvalId"`
	Decision         string `json:"decision"`
	IdempotencyKey   string `json:"idempotencyKey"`
	IdempotentReplay bool   `json:"idempotentReplay,omitempty"`
}

type ConflictError struct {
	Code    string
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

type Store struct {
	mu          sync.RWMutex
	projections map[string]Projection
	approvals   map[string]string
	decisions   map[string]ResolutionResponse
	events      map[string]struct{}
	sequences   map[string]int64
}

func NewStore() *Store {
	now := time.Now().UTC().Format(time.RFC3339)
	return &Store{
		projections: map[string]Projection{
			"org-demo": {
				OrganizationID: "org-demo", SchemaVersion: "office-projection.v1", DefinitionVersion: "definition-3", ProjectedAt: now, Source: "command-memory", Freshness: "fresh", LastEventID: "evt-107", LastSequence: 7,
				Cursors: map[string]int64{"node-01": 7}, Organization: map[string]any{"id": "org-demo", "name": "Acme Robotics"},
				Areas:     []map[string]any{{"id": "area-engineering", "name": "Engineering"}, {"id": "area-operations", "name": "Operations"}},
				Missions:  []map[string]any{{"id": "mission-release", "areaId": "area-engineering", "name": "Ship release", "status": "active"}},
				Agents:    []map[string]any{{"id": "agent-atlas", "name": "Atlas", "status": "active", "missionId": "mission-release"}},
				Nodes:     []map[string]any{{"id": "node-01", "status": "online", "lastHeartbeatAt": now}},
				Runs:      []map[string]any{{"id": "run-01", "missionId": "mission-release", "status": "waiting", "nodeId": "node-01"}},
				Approvals: []map[string]any{{"id": "approval-01", "runId": "run-01", "status": "pending", "action": "publish", "expiresAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}},
				Artifacts: []map[string]any{}, Events: []Event{{EventID: "evt-107", EventType: "approval.requested", OrganizationID: "org-demo", NodeID: "node-01", RunID: "run-01", OccurredAt: now, ReceivedAt: now, Sequence: 7, CorrelationID: "corr-01"}},
			},
		},
		approvals: map[string]string{organizationIDKey("org-demo", "approval-01"): "pending"}, decisions: make(map[string]ResolutionResponse), events: map[string]struct{}{organizationIDKey("org-demo", "evt-107"): {}}, sequences: map[string]int64{"org-demo/node-01": 7},
	}
}

func (s *Store) GetProjection(ctx context.Context, organizationID string) (Projection, bool, error) {
	if err := ctx.Err(); err != nil {
		return Projection{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	projection, ok := s.projections[organizationID]
	return projection, ok, nil
}

func (s *Store) ApplyEvent(ctx context.Context, event Event) error {
	if err := validateEvent(event); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.ReceivedAt == "" {
		event.ReceivedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	eventKey := event.OrganizationID + "\x00" + event.EventID
	if _, seen := s.events[eventKey]; seen {
		return nil
	}
	stream := event.OrganizationID + "/" + event.NodeID
	if event.Sequence <= s.sequences[stream] {
		return fmt.Errorf("event sequence %d is not after %d for %s", event.Sequence, s.sequences[stream], stream)
	}
	projection, exists := s.projections[event.OrganizationID]
	if !exists {
		projection = emptyProjection(event.OrganizationID)
		projection.Source = "command-memory"
	}
	projectionBytes, err := json.Marshal(projection)
	if err != nil {
		return fmt.Errorf("clone projection: %w", err)
	}
	if err := json.Unmarshal(projectionBytes, &projection); err != nil {
		return fmt.Errorf("clone projection: %w", err)
	}
	if err := applyProjectionEvent(&projection, event); err != nil {
		return err
	}
	s.events[eventKey] = struct{}{}
	s.sequences[stream] = event.Sequence
	if event.EventType == "approval.requested" {
		var approval map[string]any
		_ = json.Unmarshal(event.Data, &approval)
		s.approvals[organizationIDKey(event.OrganizationID, approval["id"].(string))] = "pending"
	} else if event.EventType == "approval.resolved" {
		var resolution map[string]any
		_ = json.Unmarshal(event.Data, &resolution)
		approvalID, _ := resolution["approvalId"].(string)
		decision, _ := resolution["decision"].(string)
		s.approvals[organizationIDKey(event.OrganizationID, approvalID)] = decision
	}
	s.projections[event.OrganizationID] = projection
	return nil
}

func (s *Store) ResolveApproval(ctx context.Context, approvalID string, request ResolutionRequest) (ResolutionResponse, error) {
	if err := validateResolution(request); err != nil {
		return ResolutionResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return ResolutionResponse{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idempotencyKey := request.OrganizationID + "\x00" + approvalID + "\x00" + request.IdempotencyKey
	if previous, ok := s.decisions[idempotencyKey]; ok {
		previous.IdempotentReplay = true
		return previous, nil
	}
	approvalKey := organizationIDKey(request.OrganizationID, approvalID)
	status, ok := s.approvals[approvalKey]
	if !ok {
		return ResolutionResponse{}, &ConflictError{Code: "not_found", Message: "approval not found"}
	}
	if status != "pending" {
		return ResolutionResponse{}, &ConflictError{Code: "already_decided", Message: "approval already decided"}
	}
	response := ResolutionResponse{OK: true, OrganizationID: request.OrganizationID, ApprovalID: approvalID, Decision: request.Decision, IdempotencyKey: request.IdempotencyKey}
	s.decisions[idempotencyKey] = response
	s.approvals[approvalKey] = request.Decision
	projection := s.projections[request.OrganizationID]
	for index, approval := range projection.Approvals {
		if approval["id"] == approvalID {
			approval["status"] = request.Decision
			projection.Approvals[index] = approval
		}
	}
	s.projections[request.OrganizationID] = projection
	return response, nil
}

func organizationIDKey(organizationID, id string) string {
	return organizationID + "\x00" + id
}
