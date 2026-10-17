package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/agentux-os/agentux-command/internal/office"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestRunRequiresDatabaseURL(t *testing.T) {
	var stdout, stderr strings.Builder
	if code := run(context.Background(), nil, "", &stdout, &stderr); code == 0 {
		t.Fatal("expected missing database URL to fail")
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "AGENTUX_COMMAND_DATABASE_URL") {
		t.Fatalf("expected a clear error and no report, stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunRejectsMalformedDatabaseURLWithoutLeakingIt(t *testing.T) {
	var stdout, stderr strings.Builder
	secretURL := "postgres://private-user:private-password@["
	if code := run(context.Background(), nil, secretURL, &stdout, &stderr); code == 0 {
		t.Fatal("expected malformed database URL to fail")
	}
	if stdout.Len() != 0 || strings.Contains(stderr.String(), "private-password") {
		t.Fatalf("malformed URL must not produce a report or leak credentials: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunRejectsKeywordDSNBeforeConnecting(t *testing.T) {
	var stdout, stderr strings.Builder
	if code := run(context.Background(), nil, "host=localhost user=private-user password=private-password", &stdout, &stderr); code != 2 {
		t.Fatalf("expected invalid URL to fail before connection with usage status 2, got %d; stderr=%q", code, stderr.String())
	}
	if stdout.Len() != 0 || strings.Contains(stderr.String(), "private-password") {
		t.Fatalf("invalid URL must not emit a report or credentials: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunWritesSummaryAndDetailedJSON(t *testing.T) {
	databaseURL := newReportTestDatabase(t)
	for _, test := range []struct {
		name       string
		args       []string
		wantDetail bool
	}{
		{name: "summary"},
		{name: "detailed", args: []string{"--detailed"}, wantDetail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			if code := run(context.Background(), test.args, databaseURL, &stdout, &stderr); code != 0 {
				t.Fatalf("run failed: code=%d stderr=%s", code, stderr.String())
			}
			var report map[string]any
			if err := json.Unmarshal([]byte(stdout.String()), &report); err != nil {
				t.Fatalf("stdout is not one JSON report: %v; output=%q", err, stdout.String())
			}
			_, hasDetail := report["detailed"]
			if hasDetail != test.wantDetail {
				t.Fatalf("detailed field presence=%v, want %v: %#v", hasDetail, test.wantDetail, report)
			}
			if _, ok := report["partitions"]; !ok {
				t.Fatalf("report missing partition summary: %#v", report)
			}
		})
	}
}

func TestRunQueryFailureDoesNotWritePartialReport(t *testing.T) {
	baseURL := os.Getenv("AGENTUX_COMMAND_TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("AGENTUX_COMMAND_TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", "report_missing_schema")
	parsed.RawQuery = query.Encode()
	var stdout, stderr strings.Builder
	if code := run(context.Background(), nil, parsed.String(), &stdout, &stderr); code == 0 {
		t.Fatal("expected a missing event table to fail")
	}
	if stdout.Len() != 0 || stderr.Len() == 0 {
		t.Fatalf("failed collection must not emit partial JSON: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func newReportTestDatabase(t *testing.T) string {
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
		_ = admin.Close()
		t.Fatal(err)
	}
	schema := "report_test_" + hex.EncodeToString(random)
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		_ = admin.Close()
	})
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := office.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	store := office.NewPostgresStore(db)
	if err := store.ApplyEvent(context.Background(), office.Event{EventID: "report-event", EventType: "run.started", OrganizationID: "org-report", NodeID: "node-1", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	return parsed.String()
}
