package office

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestMigrationVersionsAreOrderedAndStartAtOne(t *testing.T) {
	versions := MigrationVersions()
	want := []int{1, 2}
	if !reflect.DeepEqual(versions, want) {
		t.Fatalf("expected ordered migration versions %v, got %v", want, versions)
	}
}

func TestEventLogUsesOrganizationPartitionsAndTimeIndex(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(migrations[0].sql, "PARTITION BY HASH (organization_id)") || !strings.Contains(migrations[0].sql, "office_events_org_received_idx") {
		t.Fatal("event log must partition by organization and index received time for replay/retention queries")
	}
}

func TestOpenPostgresRejectsMissingURL(t *testing.T) {
	db, err := OpenPostgres(context.Background(), "")
	if err == nil {
		if db != nil {
			_ = db.Close()
		}
		t.Fatal("expected an error for a missing database URL")
	}
}

func TestOpenPostgresRejectsMalformedURLBeforeReturningStore(t *testing.T) {
	db, err := OpenPostgres(context.Background(), "not-a-postgres-url")
	if err == nil || db != nil {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("expected malformed URL to fail, db=%v err=%v", db, err)
	}
}
