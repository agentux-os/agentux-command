package office

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const projectionEventLimit = 100

type PostgresStore struct {
	db *sql.DB
}

var _ OfficeRepository = (*PostgresStore)(nil)

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }

func (s *PostgresStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *PostgresStore) GetProjection(ctx context.Context, organizationID string) (Projection, bool, error) {
	var encoded []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM office_projections WHERE organization_id = $1`, organizationID).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return Projection{}, false, nil
	}
	if err != nil {
		return Projection{}, false, fmt.Errorf("load office projection: %w", err)
	}
	var projection Projection
	if err := json.Unmarshal(encoded, &projection); err != nil {
		return Projection{}, false, fmt.Errorf("decode office projection: %w", err)
	}
	return projection, true, nil
}

func (s *PostgresStore) ApplyEvent(ctx context.Context, event Event) error {
	if err := validateEvent(event); err != nil {
		return err
	}
	if event.ReceivedAt == "" {
		event.ReceivedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	var approval map[string]any
	if event.EventType == "approval.requested" {
		if err := json.Unmarshal(event.Data, &approval); err != nil {
			return fmt.Errorf("decode approval event data: %w", err)
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event transaction: %w", err)
	}
	defer tx.Rollback()
	projection := emptyProjection(event.OrganizationID)
	encoded, _ := json.Marshal(projection)
	if _, err := tx.ExecContext(ctx, `INSERT INTO office_projections (organization_id, schema_version, definition_version, projected_at, source, freshness, last_event_id, last_node_id, last_sequence, payload) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (organization_id) DO NOTHING`, projection.OrganizationID, projection.SchemaVersion, projection.DefinitionVersion, event.ReceivedAt, projection.Source, projection.Freshness, projection.LastEventID, projection.LastNodeID, projection.LastSequence, encoded); err != nil {
		return fmt.Errorf("initialize office projection: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM office_projections WHERE organization_id = $1 FOR UPDATE`, event.OrganizationID).Scan(&encoded); err != nil {
		return fmt.Errorf("lock office projection: %w", err)
	}
	if err := json.Unmarshal(encoded, &projection); err != nil {
		return fmt.Errorf("decode office projection: %w", err)
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM office_events WHERE organization_id = $1 AND event_id = $2)`, event.OrganizationID, event.EventID).Scan(&exists); err != nil {
		return fmt.Errorf("check duplicate office event: %w", err)
	}
	if exists {
		return tx.Commit()
	}
	var lastSequence int64
	err = tx.QueryRowContext(ctx, `SELECT sequence FROM office_event_cursors WHERE organization_id = $1 AND node_id = $2 FOR UPDATE`, event.OrganizationID, event.NodeID).Scan(&lastSequence)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read office event cursor: %w", err)
	}
	if event.Sequence <= lastSequence {
		return &ConflictError{Code: "stale_version", Message: fmt.Sprintf("event sequence %d is not after %d for organization %s node %s", event.Sequence, lastSequence, event.OrganizationID, event.NodeID)}
	}
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode office event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO office_events (organization_id, event_id, node_id, sequence, payload) VALUES ($1,$2,$3,$4,$5)`, event.OrganizationID, event.EventID, event.NodeID, event.Sequence, eventPayload); err != nil {
		return fmt.Errorf("append office event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO office_event_cursors (organization_id, node_id, sequence, last_event_id) VALUES ($1,$2,$3,$4) ON CONFLICT (organization_id,node_id) DO UPDATE SET sequence=EXCLUDED.sequence, last_event_id=EXCLUDED.last_event_id, updated_at=now()`, event.OrganizationID, event.NodeID, event.Sequence, event.EventID); err != nil {
		return fmt.Errorf("advance office event cursor: %w", err)
	}
	if err := applyProjectionEvent(&projection, event); err != nil {
		return err
	}
	if approval != nil {
		approvalID := approval["id"].(string)
		payload, _ := json.Marshal(approval)
		if _, err := tx.ExecContext(ctx, `INSERT INTO office_approval_state (organization_id, approval_id, status, payload) VALUES ($1,$2,'pending',$3) ON CONFLICT (organization_id,approval_id) DO NOTHING`, event.OrganizationID, approvalID, payload); err != nil {
			return fmt.Errorf("record requested approval: %w", err)
		}
	}
	projectionBytes, err := json.Marshal(projection)
	if err != nil {
		return fmt.Errorf("encode office projection: %w", err)
	}
	if err := saveProjection(ctx, tx, projection, projectionBytes); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PostgresStore) ResolveApproval(ctx context.Context, approvalID string, request ResolutionRequest) (ResolutionResponse, error) {
	if err := validateResolution(request); err != nil {
		return ResolutionResponse{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ResolutionResponse{}, fmt.Errorf("begin approval transaction: %w", err)
	}
	defer tx.Rollback()
	var projectionBytes []byte
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM office_projections WHERE organization_id=$1 FOR UPDATE`, request.OrganizationID).Scan(&projectionBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ResolutionResponse{}, &ConflictError{Code: "not_found", Message: "approval not found"}
		}
		return ResolutionResponse{}, fmt.Errorf("lock approval projection: %w", err)
	}
	var status string
	var approvalBytes []byte
	if err := tx.QueryRowContext(ctx, `SELECT status,payload FROM office_approval_state WHERE organization_id=$1 AND approval_id=$2 FOR UPDATE`, request.OrganizationID, approvalID).Scan(&status, &approvalBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ResolutionResponse{}, &ConflictError{Code: "not_found", Message: "approval not found"}
		}
		return ResolutionResponse{}, fmt.Errorf("lock approval state: %w", err)
	}
	var previousBytes []byte
	err = tx.QueryRowContext(ctx, `SELECT response FROM office_approval_idempotency WHERE organization_id=$1 AND approval_id=$2 AND idempotency_key=$3`, request.OrganizationID, approvalID, request.IdempotencyKey).Scan(&previousBytes)
	if err == nil {
		var response ResolutionResponse
		if err := json.Unmarshal(previousBytes, &response); err != nil {
			return ResolutionResponse{}, fmt.Errorf("decode stored approval response: %w", err)
		}
		response.IdempotentReplay = true
		if err := tx.Commit(); err != nil {
			return ResolutionResponse{}, fmt.Errorf("finish approval replay: %w", err)
		}
		return response, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ResolutionResponse{}, fmt.Errorf("read approval idempotency record: %w", err)
	}
	if status != "pending" {
		return ResolutionResponse{}, &ConflictError{Code: "already_decided", Message: "approval already decided"}
	}
	response := ResolutionResponse{OK: true, OrganizationID: request.OrganizationID, ApprovalID: approvalID, Decision: request.Decision, IdempotencyKey: request.IdempotencyKey}
	responseBytes, err := json.Marshal(response)
	if err != nil {
		return ResolutionResponse{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO office_approval_idempotency (organization_id,approval_id,idempotency_key,response) VALUES ($1,$2,$3,$4)`, request.OrganizationID, approvalID, request.IdempotencyKey, responseBytes); err != nil {
		return ResolutionResponse{}, fmt.Errorf("persist approval idempotency response: %w", err)
	}
	decisionBytes, _ := json.Marshal(map[string]any{"decision": request.Decision, "reason": request.Reason, "idempotencyKey": request.IdempotencyKey})
	if _, err := tx.ExecContext(ctx, `UPDATE office_approval_state SET status=$3, decision=$4, updated_at=now() WHERE organization_id=$1 AND approval_id=$2`, request.OrganizationID, approvalID, request.Decision, decisionBytes); err != nil {
		return ResolutionResponse{}, fmt.Errorf("persist approval decision: %w", err)
	}
	var projection Projection
	if err := json.Unmarshal(projectionBytes, &projection); err != nil {
		return ResolutionResponse{}, fmt.Errorf("decode approval projection: %w", err)
	}
	var approval map[string]any
	if err := json.Unmarshal(approvalBytes, &approval); err != nil {
		return ResolutionResponse{}, fmt.Errorf("decode approval payload: %w", err)
	}
	resolutionEventData, _ := json.Marshal(map[string]any{"approvalId": approvalID, "decision": request.Decision, "reason": request.Reason, "idempotencyKey": request.IdempotencyKey})
	var commandSequence int64
	err = tx.QueryRowContext(ctx, `SELECT sequence FROM office_event_cursors WHERE organization_id=$1 AND node_id='command' FOR UPDATE`, request.OrganizationID).Scan(&commandSequence)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ResolutionResponse{}, fmt.Errorf("read command event cursor: %w", err)
	}
	commandSequence++
	identity := sha256.Sum256([]byte(request.OrganizationID + "\x00" + approvalID + "\x00" + request.IdempotencyKey))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	resolutionEvent := Event{EventID: "approval-resolved-" + hex.EncodeToString(identity[:]), EventType: "approval.resolved", OrganizationID: request.OrganizationID, NodeID: "command", OccurredAt: now, ReceivedAt: now, Sequence: commandSequence, Data: resolutionEventData}
	if err := applyProjectionEvent(&projection, resolutionEvent); err != nil {
		return ResolutionResponse{}, err
	}
	resolutionEventBytes, _ := json.Marshal(resolutionEvent)
	if _, err := tx.ExecContext(ctx, `INSERT INTO office_events (organization_id,event_id,node_id,sequence,payload) VALUES ($1,$2,$3,$4,$5)`, request.OrganizationID, resolutionEvent.EventID, resolutionEvent.NodeID, resolutionEvent.Sequence, resolutionEventBytes); err != nil {
		return ResolutionResponse{}, fmt.Errorf("append approval resolution event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO office_event_cursors (organization_id,node_id,sequence,last_event_id) VALUES ($1,$2,$3,$4) ON CONFLICT (organization_id,node_id) DO UPDATE SET sequence=EXCLUDED.sequence,last_event_id=EXCLUDED.last_event_id,updated_at=now()`, request.OrganizationID, resolutionEvent.NodeID, resolutionEvent.Sequence, resolutionEvent.EventID); err != nil {
		return ResolutionResponse{}, fmt.Errorf("advance approval event cursor: %w", err)
	}
	approval["status"] = request.Decision
	approvalBytes, _ = json.Marshal(approval)
	if _, err := tx.ExecContext(ctx, `UPDATE office_approval_state SET payload=$3 WHERE organization_id=$1 AND approval_id=$2`, request.OrganizationID, approvalID, approvalBytes); err != nil {
		return ResolutionResponse{}, fmt.Errorf("update approval payload: %w", err)
	}
	projectionBytes, err = json.Marshal(projection)
	if err != nil {
		return ResolutionResponse{}, fmt.Errorf("encode approval projection: %w", err)
	}
	if err := saveProjection(ctx, tx, projection, projectionBytes); err != nil {
		return ResolutionResponse{}, err
	}
	if err := tx.Commit(); err != nil {
		return ResolutionResponse{}, fmt.Errorf("commit approval decision: %w", err)
	}
	return response, nil
}

func (s *PostgresStore) VerifyProjection(ctx context.Context, organizationID string) error {
	stored, ok, err := s.GetProjection(ctx, organizationID)
	if err != nil {
		return err
	}
	if !ok {
		return &ConflictError{Code: "not_found", Message: "projection not found"}
	}
	rebuilt := emptyProjection(organizationID)
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM office_events WHERE organization_id=$1 ORDER BY ingest_id`, organizationID)
	if err != nil {
		return fmt.Errorf("read event log for replay: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var encoded []byte
		var event Event
		if err := rows.Scan(&encoded); err != nil {
			return fmt.Errorf("read replay event: %w", err)
		}
		if err := json.Unmarshal(encoded, &event); err != nil {
			return fmt.Errorf("decode replay event: %w", err)
		}
		if err := applyProjectionEvent(&rebuilt, event); err != nil {
			return fmt.Errorf("apply replay event %s: %w", event.EventID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate replay events: %w", err)
	}
	if !reflect.DeepEqual(stored, rebuilt) {
		return fmt.Errorf("materialized projection for organization %s differs from event replay", organizationID)
	}
	return nil
}

func validateEvent(event Event) error {
	if event.EventID == "" || event.OrganizationID == "" || event.Sequence < 1 {
		return fmt.Errorf("event requires eventId, organizationId and positive sequence")
	}
	if event.EventType == "approval.requested" || event.EventType == "approval.resolved" {
		var data map[string]any
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("decode %s data: %w", event.EventType, err)
		}
		key := "id"
		if event.EventType == "approval.resolved" {
			key = "approvalId"
		}
		if id, _ := data[key].(string); id == "" {
			return fmt.Errorf("%s data requires %s", event.EventType, key)
		}
		if event.EventType == "approval.resolved" {
			decision, _ := data["decision"].(string)
			if decision != "approved" && decision != "rejected" {
				return fmt.Errorf("approval.resolved data has invalid decision")
			}
		}
	}
	return nil
}

func validateResolution(request ResolutionRequest) error {
	if request.OrganizationID == "" {
		return &ConflictError{Code: "stale_version", Message: "organizationId is required"}
	}
	if request.Decision != "approved" && request.Decision != "rejected" {
		return &ConflictError{Code: "denied", Message: "decision must be approved or rejected"}
	}
	if request.IdempotencyKey == "" {
		return &ConflictError{Code: "stale_version", Message: "idempotencyKey is required"}
	}
	return nil
}

func emptyProjection(organizationID string) Projection {
	return Projection{OrganizationID: organizationID, SchemaVersion: "office-projection.v1", DefinitionVersion: "definition-unknown", ProjectedAt: time.Now().UTC().Format(time.RFC3339Nano), Source: "command-postgres", Freshness: "fresh", Organization: map[string]any{"id": organizationID}, Cursors: map[string]int64{}, Areas: []map[string]any{}, Missions: []map[string]any{}, Agents: []map[string]any{}, Nodes: []map[string]any{}, Runs: []map[string]any{}, Approvals: []map[string]any{}, Artifacts: []map[string]any{}, Events: []Event{}}
}

func applyProjectionEvent(projection *Projection, event Event) error {
	projection.OrganizationID = event.OrganizationID
	projection.SchemaVersion = "office-projection.v1"
	if projection.DefinitionVersion == "" {
		projection.DefinitionVersion = "definition-unknown"
	}
	projection.ProjectedAt = event.ReceivedAt
	if projection.Source == "" {
		projection.Source = "command-postgres"
	}
	projection.Freshness = "fresh"
	projection.LastEventID = event.EventID
	projection.LastNodeID = event.NodeID
	projection.LastSequence = event.Sequence
	if projection.Cursors == nil {
		projection.Cursors = map[string]int64{}
	}
	if current := projection.Cursors[event.NodeID]; event.Sequence > current {
		projection.Cursors[event.NodeID] = event.Sequence
	}
	projection.Events = append(projection.Events, event)
	if len(projection.Events) > projectionEventLimit {
		projection.Events = append([]Event(nil), projection.Events[len(projection.Events)-projectionEventLimit:]...)
	}
	if event.EventType == "approval.requested" {
		var approval map[string]any
		if err := json.Unmarshal(event.Data, &approval); err != nil {
			return err
		}
		approval["status"] = "pending"
		projection.Approvals = append(projection.Approvals, approval)
	}
	if event.EventType == "approval.resolved" {
		var resolution map[string]any
		if err := json.Unmarshal(event.Data, &resolution); err != nil {
			return err
		}
		for index, approval := range projection.Approvals {
			if approval["id"] == resolution["approvalId"] {
				approval["status"] = resolution["decision"]
				projection.Approvals[index] = approval
			}
		}
	}
	return nil
}

func saveProjection(ctx context.Context, tx *sql.Tx, projection Projection, encoded []byte) error {
	if _, err := tx.ExecContext(ctx, `UPDATE office_projections SET schema_version=$2,definition_version=$3,projected_at=$4,source=$5,freshness=$6,last_event_id=$7,last_node_id=$8,last_sequence=$9,payload=$10 WHERE organization_id=$1`, projection.OrganizationID, projection.SchemaVersion, projection.DefinitionVersion, projection.ProjectedAt, projection.Source, projection.Freshness, projection.LastEventID, projection.LastNodeID, projection.LastSequence, encoded); err != nil {
		return fmt.Errorf("save office projection: %w", err)
	}
	return nil
}
