package office

import (
	"context"
	"reflect"
	"testing"
)

func TestMigrationVersionsAreOrderedAndStartAtOne(t *testing.T) {
	versions := MigrationVersions()
	want := []int{1}
	if !reflect.DeepEqual(versions, want) {
		t.Fatalf("expected ordered migration versions %v, got %v", want, versions)
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
