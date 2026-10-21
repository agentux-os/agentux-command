package enroll

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// stores runs the same checks on every Store: memory always, PostgreSQL
// when AGENTUX_COMMAND_TEST_DATABASE_URL is set (the tables are created by
// office.Migrate; here they are reset).
func stores(t *testing.T) map[string]Store {
	t.Helper()
	out := map[string]Store{"memory": NewMemory("")}
	url := os.Getenv("AGENTUX_COMMAND_TEST_DATABASE_URL")
	if url == "" {
		return out
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) != "" {
			if _, err := db.Exec(statement); err != nil {
				t.Fatalf("schema: %v", err)
			}
		}
	}
	for _, table := range []string{"node_credentials", "enrollment_tokens", "command_instance"} {
		if _, err := db.Exec("DELETE FROM " + table); err != nil {
			t.Fatal(err)
		}
	}
	out["postgres"] = NewPostgres(db)
	return out
}

// The tables of migration 002, so the package tests stand alone.
const schema = `
CREATE TABLE IF NOT EXISTS command_instance (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), instance_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS enrollment_tokens (token_hash text PRIMARY KEY, organization_id text NOT NULL, node_name text NOT NULL DEFAULT '', require_approval boolean NOT NULL DEFAULT false, expires_at timestamptz NOT NULL, created_by text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now(), used_at timestamptz, used_by_node text);
CREATE TABLE IF NOT EXISTS node_credentials (node_id text PRIMARY KEY, organization_id text NOT NULL, secret_hash text NOT NULL UNIQUE, hostname text NOT NULL DEFAULT '', approved boolean NOT NULL, enrolled_at timestamptz NOT NULL DEFAULT now(), approved_at timestamptz, revoked_at timestamptz)
`

func TestTokensAreBoundToOneInstanceUsedOnceAndExpire(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			instance, err := store.InstanceID(ctx)
			if err != nil || !ValidInstanceID(instance) {
				t.Fatalf("instance %q: %v", instance, err)
			}
			again, _ := store.InstanceID(ctx)
			if again != instance {
				t.Fatal("the instance id must not change")
			}
			tokens, err := store.IssueTokens(ctx, TokenRequest{OrganizationID: "acme", Count: 3, TTL: time.Hour}, now)
			if err != nil || len(tokens) != 3 {
				t.Fatalf("issue: %v", err)
			}
			if got, _ := TokenInstance(tokens[0].Token); got != instance {
				t.Fatalf("the token must name this instance: %s", tokens[0].Token)
			}

			first, err := store.Redeem(ctx, tokens[0].Token, "", "Build Box 1", now)
			if err != nil || first.Credential.NodeID != "node-build-box-1" || !first.Credential.Approved || first.Credential.OrganizationID != "acme" {
				t.Fatalf("redeem: %+v %v", first.Credential, err)
			}
			if _, err := store.Redeem(ctx, tokens[0].Token, "", "x", now); !errors.Is(err, ErrUsedToken) {
				t.Fatalf("a used token must fail, got %v", err)
			}
			// Same host name: a unique id, never someone else's.
			second, err := store.Redeem(ctx, tokens[1].Token, "", "Build Box 1", now)
			if err != nil || second.Credential.NodeID != "node-build-box-1-2" {
				t.Fatalf("expected a suffixed id, got %+v %v", second.Credential, err)
			}
			if _, err := store.Redeem(ctx, tokens[2].Token, "", "x", now.Add(2*time.Hour)); !errors.Is(err, ErrExpiredToken) {
				t.Fatalf("an expired token must fail, got %v", err)
			}

			// Another Command's token, or a forged one.
			other := NewMemory("")
			foreign, _ := other.IssueTokens(ctx, TokenRequest{OrganizationID: "acme"}, now)
			if _, err := store.Redeem(ctx, foreign[0].Token, "", "x", now); !errors.Is(err, ErrOtherInstance) {
				t.Fatalf("another instance's token must fail, got %v", err)
			}
			forged := TokenPrefix + instance + "_" + strings.Repeat("0", 64)
			if _, err := store.Redeem(ctx, forged, "", "x", now); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("a forged token must fail, got %v", err)
			}

			// The credential authenticates its node only, until revoked.
			if c, err := store.Authenticate(ctx, first.Credential.NodeID, first.Secret); err != nil || !c.Active() {
				t.Fatalf("authenticate: %+v %v", c, err)
			}
			if _, err := store.Authenticate(ctx, second.Credential.NodeID, first.Secret); err == nil {
				t.Fatal("a node's secret must not authenticate another node")
			}
			if err := store.Revoke(ctx, first.Credential.NodeID, now); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Authenticate(ctx, first.Credential.NodeID, first.Secret); err == nil {
				t.Fatal("a revoked node must not authenticate")
			}
		})
	}
}

func TestNamedTokensAndApproval(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			if _, err := store.IssueTokens(ctx, TokenRequest{OrganizationID: "acme", NodeName: "gpu-1", Count: 2}, now); err == nil {
				t.Fatal("a named token is issued alone")
			}
			if _, err := store.IssueTokens(ctx, TokenRequest{OrganizationID: "bad org"}, now); err == nil {
				t.Fatal("organization ids are checked")
			}
			named, _ := store.IssueTokens(ctx, TokenRequest{OrganizationID: "acme", NodeName: "gpu-1", RequireApproval: true}, now)
			r, err := store.Redeem(ctx, named[0].Token, "whatever", "host", now)
			if err != nil || r.Credential.NodeID != "gpu-1" || r.Credential.Approved {
				t.Fatalf("expected gpu-1 waiting for approval, got %+v %v", r.Credential, err)
			}
			if c, _ := store.Authenticate(ctx, "gpu-1", r.Secret); c.Active() {
				t.Fatal("a node waiting for approval is not active")
			}
			if err := store.Approve(ctx, "gpu-1", now); err != nil {
				t.Fatal(err)
			}
			if c, _ := store.Authenticate(ctx, "gpu-1", r.Secret); !c.Active() {
				t.Fatal("an approved node is active")
			}
			// A second token for the same name waits until gpu-1 is revoked.
			again, _ := store.IssueTokens(ctx, TokenRequest{OrganizationID: "acme", NodeName: "gpu-1"}, now)
			if _, err := store.Redeem(ctx, again[0].Token, "", "", now); err == nil {
				t.Fatal("an enrolled name must not be taken over")
			}
			_ = store.Revoke(ctx, "gpu-1", now)
			again, _ = store.IssueTokens(ctx, TokenRequest{OrganizationID: "acme", NodeName: "gpu-1"}, now)
			if r, err := store.Redeem(ctx, again[0].Token, "", "", now); err != nil || r.Credential.NodeID != "gpu-1" {
				t.Fatalf("a revoked name may enroll again: %v", err)
			}
		})
	}
}
