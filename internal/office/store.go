package office

import (
	"fmt"
	"sync"
	"time"
)

type Event struct {
	EventID        string `json:"eventId"`
	EventType      string `json:"eventType"`
	OrganizationID string `json:"organizationId"`
	NodeID         string `json:"nodeId,omitempty"`
	RunID          string `json:"runId,omitempty"`
	OccurredAt     string `json:"occurredAt,omitempty"`
	ReceivedAt     string `json:"receivedAt,omitempty"`
	Sequence       int64  `json:"sequence"`
	CorrelationID  string `json:"correlationId,omitempty"`
	CausationID    string `json:"causationId,omitempty"`
}

type Projection struct {
	OrganizationID    string           `json:"organizationId"`
	SchemaVersion     string           `json:"schemaVersion"`
	DefinitionVersion string           `json:"definitionVersion"`
	ProjectedAt       string           `json:"projectedAt"`
	Source            string           `json:"source"`
	Freshness         string           `json:"freshness"`
	LastEventID       string           `json:"lastEventId"`
	LastSequence      int64            `json:"lastSequence,omitempty"`
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
	Decision       string `json:"decision"`
	Reason         string `json:"reason,omitempty"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type ResolutionResponse struct {
	OK               bool   `json:"ok"`
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
				Organization: map[string]any{"id": "org-demo", "name": "Acme Robotics"},
				Areas:        []map[string]any{{"id": "area-engineering", "name": "Engineering"}, {"id": "area-operations", "name": "Operations"}},
				Missions:     []map[string]any{{"id": "mission-release", "areaId": "area-engineering", "name": "Ship release", "status": "active"}},
				Agents:       []map[string]any{{"id": "agent-atlas", "name": "Atlas", "status": "active", "missionId": "mission-release"}},
				Nodes:        []map[string]any{{"id": "node-01", "status": "online", "lastHeartbeatAt": now}},
				Runs:         []map[string]any{{"id": "run-01", "missionId": "mission-release", "status": "waiting", "nodeId": "node-01"}},
				Approvals:    []map[string]any{{"id": "approval-01", "runId": "run-01", "status": "pending", "action": "publish", "expiresAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}},
				Artifacts:    []map[string]any{}, Events: []Event{{EventID: "evt-107", EventType: "approval.requested", OrganizationID: "org-demo", NodeID: "node-01", RunID: "run-01", OccurredAt: now, ReceivedAt: now, Sequence: 7, CorrelationID: "corr-01"}},
			},
		},
		approvals: map[string]string{"approval-01": "pending"}, decisions: make(map[string]ResolutionResponse), events: make(map[string]struct{}), sequences: make(map[string]int64),
	}
}

func (s *Store) GetProjection(organizationID string) (Projection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	projection, ok := s.projections[organizationID]
	return projection, ok
}

func (s *Store) ApplyEvent(event Event) error {
	if event.EventID == "" || event.OrganizationID == "" || event.Sequence < 1 {
		return fmt.Errorf("event requires eventId, organizationId and positive sequence")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, seen := s.events[event.EventID]; seen {
		return nil
	}
	stream := event.OrganizationID + "/" + event.NodeID
	if event.Sequence <= s.sequences[stream] {
		return fmt.Errorf("event sequence %d is not after %d for %s", event.Sequence, s.sequences[stream], stream)
	}
	s.events[event.EventID] = struct{}{}
	s.sequences[stream] = event.Sequence
	projection := s.projections[event.OrganizationID]
	projection.OrganizationID = event.OrganizationID
	projection.SchemaVersion = "office-projection.v1"
	projection.DefinitionVersion = "definition-3"
	projection.ProjectedAt = time.Now().UTC().Format(time.RFC3339)
	projection.Source = "command-memory"
	projection.Freshness = "fresh"
	projection.LastEventID = event.EventID
	projection.LastSequence = event.Sequence
	projection.Events = append(projection.Events, event)
	s.projections[event.OrganizationID] = projection
	return nil
}

func (s *Store) ResolveApproval(approvalID string, request ResolutionRequest) (ResolutionResponse, error) {
	if request.Decision != "approved" && request.Decision != "rejected" {
		return ResolutionResponse{}, &ConflictError{Code: "denied", Message: "decision must be approved or rejected"}
	}
	if request.IdempotencyKey == "" {
		return ResolutionResponse{}, &ConflictError{Code: "stale_version", Message: "idempotencyKey is required"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, ok := s.decisions[request.IdempotencyKey]; ok {
		previous.IdempotentReplay = true
		return previous, nil
	}
	status, ok := s.approvals[approvalID]
	if !ok {
		return ResolutionResponse{}, &ConflictError{Code: "not_found", Message: "approval not found"}
	}
	if status != "pending" {
		return ResolutionResponse{}, &ConflictError{Code: "already_decided", Message: "approval already decided"}
	}
	response := ResolutionResponse{OK: true, ApprovalID: approvalID, Decision: request.Decision, IdempotencyKey: request.IdempotencyKey}
	s.decisions[request.IdempotencyKey] = response
	s.approvals[approvalID] = request.Decision
	return response, nil
}
