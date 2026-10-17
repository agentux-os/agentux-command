package office

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestGrowthReportJSONOmitsDetailedDataUnlessRequested(t *testing.T) {
	collectedAt := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	report := GrowthReport{
		Database:           "agentux",
		CollectedAt:        collectedAt,
		TotalEventBytes:    4096,
		EstimatedEventRows: 7,
		Partitions:         []PartitionGrowth{{Name: "office_events_p0", Bytes: 1024, EstimatedRows: 1}},
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	if strings.Contains(text, "organizations") || strings.Contains(text, "oldestReceivedAt") {
		t.Fatalf("summary report must omit detailed tenant/time fields: %s", text)
	}
	if !strings.Contains(text, `"database":"agentux"`) || !strings.Contains(text, `"totalEventBytes":4096`) {
		t.Fatalf("summary report is missing expected fields: %s", text)
	}

	report.Detailed = &DetailedGrowth{Organizations: []OrganizationGrowth{{OrganizationID: "org-a", EventCount: 7}}}
	encoded, err = json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"organizationId":"org-a"`) {
		t.Fatalf("detailed report must include tenant aggregates when requested: %s", encoded)
	}
}

func TestCollectGrowthReportRejectsNilDatabase(t *testing.T) {
	report, err := CollectGrowthReport(context.Background(), nil, false)
	if err == nil {
		t.Fatal("expected a nil database to return an error")
	}
	if report.Database != "" || report.Partitions != nil {
		t.Fatalf("failed report should not contain partial data: %#v", report)
	}
}
