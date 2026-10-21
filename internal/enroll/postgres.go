package enroll

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Postgres is a Store in Command's database (migration 002_node_enrollment).
type Postgres struct {
	db *sql.DB
}

func NewPostgres(db *sql.DB) *Postgres { return &Postgres{db: db} }

// InstanceID is created on first use and kept for the database's life.
func (p *Postgres) InstanceID(ctx context.Context) (string, error) {
	if _, err := p.db.ExecContext(ctx, `INSERT INTO command_instance (singleton, instance_id) VALUES (true, $1) ON CONFLICT (singleton) DO NOTHING`, NewInstanceID()); err != nil {
		return "", fmt.Errorf("create the instance id: %w", err)
	}
	var id string
	if err := p.db.QueryRowContext(ctx, `SELECT instance_id FROM command_instance WHERE singleton`).Scan(&id); err != nil {
		return "", fmt.Errorf("read the instance id: %w", err)
	}
	return id, nil
}

func (p *Postgres) IssueTokens(ctx context.Context, request TokenRequest, now time.Time) ([]IssuedToken, error) {
	if err := validateRequest(&request); err != nil {
		return nil, err
	}
	instance, err := p.InstanceID(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	issued := make([]IssuedToken, 0, request.Count)
	for i := 0; i < request.Count; i++ {
		token := newToken(instance)
		expires := now.Add(request.TTL).UTC()
		if _, err := tx.ExecContext(ctx, `INSERT INTO enrollment_tokens (token_hash, organization_id, node_name, require_approval, expires_at, created_by) VALUES ($1, $2, $3, $4, $5, $6)`,
			hash(token), request.OrganizationID, request.NodeName, request.RequireApproval, expires, request.CreatedBy); err != nil {
			return nil, fmt.Errorf("store a token: %w", err)
		}
		issued = append(issued, IssuedToken{Token: token, ExpiresAt: expires, NodeName: request.NodeName})
	}
	return issued, tx.Commit()
}

func (p *Postgres) Redeem(ctx context.Context, token, nodeName, hostname string, now time.Time) (Redemption, error) {
	tokenInstance, ok := TokenInstance(token)
	if !ok {
		return Redemption{}, ErrInvalidToken
	}
	instance, err := p.InstanceID(ctx)
	if err != nil {
		return Redemption{}, err
	}
	if tokenInstance != instance {
		return Redemption{}, ErrOtherInstance
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return Redemption{}, err
	}
	defer tx.Rollback()
	var record tokenRecord
	var usedAt sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT organization_id, node_name, require_approval, expires_at, used_at FROM enrollment_tokens WHERE token_hash = $1 FOR UPDATE`, hash(token)).
		Scan(&record.OrganizationID, &record.NodeName, &record.RequireApproval, &record.ExpiresAt, &usedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Redemption{}, ErrInvalidToken
	case err != nil:
		return Redemption{}, err
	case usedAt.Valid:
		return Redemption{}, ErrUsedToken
	case !now.Before(record.ExpiresAt):
		return Redemption{}, ErrExpiredToken
	}
	// Node ids are taken one enrollment at a time.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(842120)`); err != nil {
		return Redemption{}, err
	}
	var lookupErr error
	id, err := nodeID(record.NodeName, nodeName, hostname, func(id string) bool {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM node_credentials WHERE node_id = $1 AND revoked_at IS NULL)`, id).Scan(&exists); err != nil {
			lookupErr = err
			return true
		}
		return exists
	})
	if lookupErr != nil {
		return Redemption{}, lookupErr
	}
	if err != nil {
		return Redemption{}, err
	}
	secret := newSecret()
	approved := !record.RequireApproval
	// A revoked node's id may be enrolled again.
	if _, err := tx.ExecContext(ctx, `INSERT INTO node_credentials (node_id, organization_id, secret_hash, hostname, approved, enrolled_at, approved_at)
		VALUES ($1, $2, $3, $4, $5, $6, CASE WHEN $5 THEN $6::timestamptz END)
		ON CONFLICT (node_id) DO UPDATE SET organization_id = EXCLUDED.organization_id, secret_hash = EXCLUDED.secret_hash,
			hostname = EXCLUDED.hostname, approved = EXCLUDED.approved, enrolled_at = EXCLUDED.enrolled_at,
			approved_at = EXCLUDED.approved_at, revoked_at = NULL
		WHERE node_credentials.revoked_at IS NOT NULL`,
		id, record.OrganizationID, hash(secret), hostname, approved, now.UTC()); err != nil {
		return Redemption{}, fmt.Errorf("store the node: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE enrollment_tokens SET used_at = $2, used_by_node = $3 WHERE token_hash = $1`, hash(token), now.UTC(), id); err != nil {
		return Redemption{}, err
	}
	if err := tx.Commit(); err != nil {
		return Redemption{}, err
	}
	return Redemption{
		Credential: Credential{NodeID: id, OrganizationID: record.OrganizationID, Hostname: hostname, Approved: approved, EnrolledAt: now.UTC()},
		Secret:     secret,
	}, nil
}

func (p *Postgres) Authenticate(ctx context.Context, nodeID, secret string) (Credential, error) {
	var c Credential
	var stored string
	var revoked sql.NullTime
	err := p.db.QueryRowContext(ctx, `SELECT node_id, organization_id, hostname, approved, enrolled_at, revoked_at, secret_hash FROM node_credentials WHERE node_id = $1`, nodeID).
		Scan(&c.NodeID, &c.OrganizationID, &c.Hostname, &c.Approved, &c.EnrolledAt, &revoked, &stored)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, ErrUnknownNode
	}
	if err != nil {
		return Credential{}, err
	}
	if revoked.Valid || subtle.ConstantTimeCompare([]byte(stored), []byte(hash(secret))) != 1 {
		return Credential{}, ErrUnknownNode
	}
	return c, nil
}

func (p *Postgres) Credentials(ctx context.Context) ([]Credential, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT node_id, organization_id, hostname, approved, enrolled_at, revoked_at FROM node_credentials ORDER BY node_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Credential
	for rows.Next() {
		var c Credential
		var revoked sql.NullTime
		if err := rows.Scan(&c.NodeID, &c.OrganizationID, &c.Hostname, &c.Approved, &c.EnrolledAt, &revoked); err != nil {
			return nil, err
		}
		if revoked.Valid {
			at := revoked.Time
			c.RevokedAt = &at
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *Postgres) Approve(ctx context.Context, nodeID string, now time.Time) error {
	result, err := p.db.ExecContext(ctx, `UPDATE node_credentials SET approved = true, approved_at = $2 WHERE node_id = $1 AND revoked_at IS NULL`, nodeID, now.UTC())
	return affected(result, err)
}

func (p *Postgres) Revoke(ctx context.Context, nodeID string, now time.Time) error {
	result, err := p.db.ExecContext(ctx, `UPDATE node_credentials SET revoked_at = $2 WHERE node_id = $1 AND revoked_at IS NULL`, nodeID, now.UTC())
	return affected(result, err)
}

func affected(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return ErrUnknownNode
	}
	return nil
}
