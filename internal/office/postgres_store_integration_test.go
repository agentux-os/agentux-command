package office

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func newPostgresTestStore(t *testing.T) (*PostgresStore, string) {
	t.Helper()
	databaseURL := os.Getenv("AGENTUX_COMMAND_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGENTUX_COMMAND_TEST_DATABASE_URL is not set")
	}
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Ping(); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	schema := "office_test_" + hex.EncodeToString(random)
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		_ = admin.Close()
	})
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = db.Close() })
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return NewPostgresStore(db), databaseURL
}

func reopenPostgresTestStore(t *testing.T, databaseURL, schema string) *PostgresStore {
	t.Helper()
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = db.Close() })
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return NewPostgresStore(db)
}

func createTestSchema(t *testing.T, databaseURL string) string {
	t.Helper()
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	schema := "office_test_" + hex.EncodeToString(random)
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, err := sql.Open("pgx", databaseURL)
		if err == nil {
			_, _ = cleanup.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
			_ = cleanup.Close()
		}
	})
	return schema
}

func TestPostgresStoreRecoversAndDeduplicatesEventsAcrossConnections(t *testing.T) {
	store, databaseURL := newPostgresTestStore(t)
	projection := Event{EventID: "evt-restart", EventType: "run.started", OrganizationID: "org-a", NodeID: "node-01", Sequence: 11}
	if err := store.ApplyEvent(context.Background(), projection); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := store.db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	restarted := reopenPostgresTestStore(t, databaseURL, schema)
	if err := restarted.ApplyEvent(context.Background(), projection); err != nil {
		t.Fatalf("duplicate replay should be idempotent: %v", err)
	}
	loaded, ok, err := restarted.GetProjection(context.Background(), "org-a")
	if err != nil || !ok || loaded.LastEventID != "evt-restart" || loaded.LastSequence != 11 || len(loaded.Events) != 1 {
		t.Fatalf("expected durable projection after restart, got %#v ok=%v err=%v", loaded, ok, err)
	}
	if err := restarted.VerifyProjection(context.Background(), "org-a"); err != nil {
		t.Fatalf("projection should verify against event log: %v", err)
	}
}

func TestPostgresStoreRejectsOutOfOrderNodeEvent(t *testing.T) {
	store, _ := newPostgresTestStore(t)
	ctx := context.Background()
	if err := store.ApplyEvent(ctx, Event{EventID: "evt-12", EventType: "run.updated", OrganizationID: "org-a", NodeID: "node-01", Sequence: 12}); err != nil {
		t.Fatal(err)
	}
	err := store.ApplyEvent(ctx, Event{EventID: "evt-10", EventType: "run.updated", OrganizationID: "org-a", NodeID: "node-01", Sequence: 10})
	if conflict, ok := err.(*ConflictError); !ok || conflict.Code != "stale_version" {
		t.Fatalf("expected stale cursor conflict, got %v", err)
	}
}

func TestPostgresStoreScopesApprovalIdempotencyAndSerializesConcurrentRetries(t *testing.T) {
	store, _ := newPostgresTestStore(t)
	ctx := context.Background()
	for _, orgID := range []string{"org-a", "org-b"} {
		data, _ := json.Marshal(map[string]any{"id": "approval-1", "action": "publish", "expiresAt": "2099-01-01T00:00:00Z"})
		if err := store.ApplyEvent(ctx, Event{EventID: "requested-" + orgID, EventType: "approval.requested", OrganizationID: orgID, NodeID: "node-01", Sequence: 1, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	request := ResolutionRequest{OrganizationID: "org-a", Decision: "approved", IdempotencyKey: "shared-key"}
	const retries = 8
	responses := make([]ResolutionResponse, retries)
	errorsByIndex := make([]error, retries)
	var wait sync.WaitGroup
	for i := range responses {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			responses[index], errorsByIndex[index] = store.ResolveApproval(ctx, "approval-1", request)
		}(i)
	}
	wait.Wait()
	for i, err := range errorsByIndex {
		if err != nil {
			t.Fatalf("retry %d failed: %v", i, err)
		}
		if responses[i].Decision != "approved" || responses[i].OrganizationID != "org-a" {
			t.Fatalf("retry %d returned wrong decision: %#v", i, responses[i])
		}
	}

	other, err := store.ResolveApproval(ctx, "approval-1", ResolutionRequest{OrganizationID: "org-b", Decision: "rejected", IdempotencyKey: "shared-key"})
	if err != nil || other.IdempotentReplay || other.Decision != "rejected" {
		t.Fatalf("same idempotency key in another organization must be independent: %#v err=%v", other, err)
	}
}

func TestPostgresStoreReplayVerificationDetectsProjectionDrift(t *testing.T) {
	store, _ := newPostgresTestStore(t)
	ctx := context.Background()
	if err := store.ApplyEvent(ctx, Event{EventID: "evt-drift", EventType: "run.started", OrganizationID: "org-a", NodeID: "node-01", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE office_projections SET payload = jsonb_set(payload, '{freshness}', '"stale"') WHERE organization_id = $1`, "org-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifyProjection(ctx, "org-a"); err == nil {
		t.Fatal("expected replay verification to detect projection drift")
	}
}

func TestPostgresStoreCanMigrateAndOpenConfiguredDatabase(t *testing.T) {
	databaseURL := os.Getenv("AGENTUX_COMMAND_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGENTUX_COMMAND_TEST_DATABASE_URL is not set")
	}
	schema := createTestSchema(t, databaseURL)
	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	parsedURL.RawQuery = query.Encode()
	dbURL := parsedURL.String()
	db, err := OpenPostgres(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := Migrate(context.Background(), db); err != nil {
		_ = db.Close()
		t.Fatal(fmt.Errorf("repeat migration should be safe: %w", err))
	}
	_ = db.Close()
}

func TestPostgresGrowthReportIncludesPartitionSummaryAndOptionalOrganizationAggregates(t *testing.T) {
	store, _ := newPostgresTestStore(t)
	ctx := context.Background()
	for i, orgID := range []string{"org-growth-a", "org-growth-b"} {
		if err := store.ApplyEvent(ctx, Event{EventID: "growth-" + orgID, EventType: "run.started", OrganizationID: orgID, NodeID: "node-01", Sequence: int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}

	summary, err := CollectGrowthReport(ctx, store.db, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Database == "" || summary.CollectedAt.IsZero() || summary.TotalEventBytes <= 0 || len(summary.Partitions) != 8 {
		t.Fatalf("summary missing database, time, size, or partition data: %#v", summary)
	}
	if summary.Detailed != nil {
		t.Fatalf("summary mode must not include organization details: %#v", summary.Detailed)
	}

	detailed, err := CollectGrowthReport(ctx, store.db, true)
	if err != nil {
		t.Fatal(err)
	}
	if detailed.Detailed == nil || len(detailed.Detailed.Organizations) != 2 {
		t.Fatalf("detailed report should contain both organization aggregates: %#v", detailed.Detailed)
	}
	for _, organization := range detailed.Detailed.Organizations {
		if organization.EventCount != 1 || organization.OldestReceivedAt == nil || organization.NewestReceivedAt == nil {
			t.Fatalf("organization aggregate has wrong count or time bounds: %#v", organization)
		}
	}
}

func TestPostgresGrowthReportHandlesEmptyEventHistory(t *testing.T) {
	store, _ := newPostgresTestStore(t)
	report, err := CollectGrowthReport(context.Background(), store.db, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Detailed == nil || report.Detailed.OldestReceivedAt != nil || report.Detailed.NewestReceivedAt != nil || len(report.Detailed.Organizations) != 0 {
		t.Fatalf("empty event history should produce null bounds and no organizations: %#v", report.Detailed)
	}
}
