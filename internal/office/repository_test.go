package office

import (
	"context"
	"testing"
)

var _ OfficeRepository = (*Store)(nil)

func TestStoreScopesEventDeduplicationByOrganization(t *testing.T) {
	store := NewStore()
	for _, organizationID := range []string{"org-a", "org-b"} {
		err := store.ApplyEvent(context.Background(), Event{EventID: "evt-shared", EventType: "run.started", OrganizationID: organizationID, NodeID: "node-01", Sequence: 1})
		if err != nil {
			t.Fatalf("apply event for %s: %v", organizationID, err)
		}
		projection, ok, err := store.GetProjection(context.Background(), organizationID)
		if err != nil || !ok || projection.LastEventID != "evt-shared" {
			t.Fatalf("expected event in organization %s projection; projection=%#v ok=%v err=%v", organizationID, projection, ok, err)
		}
	}
}

func TestStoreScopesApprovalIdempotencyByOrganizationAndApproval(t *testing.T) {
	store := NewStore()
	request := ResolutionRequest{OrganizationID: "org-demo", Decision: "approved", IdempotencyKey: "shared-key"}
	first, err := store.ResolveApproval(context.Background(), "approval-01", request)
	if err != nil || first.IdempotentReplay {
		t.Fatalf("expected original decision, response=%#v err=%v", first, err)
	}
	request.OrganizationID = "org-other"
	_, err = store.ResolveApproval(context.Background(), "approval-01", request)
	if conflict, ok := err.(*ConflictError); !ok || conflict.Code != "not_found" {
		t.Fatalf("expected org-other not-found rather than replaying org-demo decision, got %v", err)
	}
}

func TestStoreProjectionKeepsIndependentNodeCursors(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	for _, event := range []Event{
		{EventID: "evt-node-a", EventType: "run.updated", OrganizationID: "org-demo", NodeID: "node-a", Sequence: 9},
		{EventID: "evt-node-b", EventType: "run.updated", OrganizationID: "org-demo", NodeID: "node-b", Sequence: 3},
	} {
		if err := store.ApplyEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	projection, ok, err := store.GetProjection(ctx, "org-demo")
	if err != nil || !ok || projection.Cursors["node-a"] != 9 || projection.Cursors["node-b"] != 3 {
		t.Fatalf("expected independent per-node cursors, got %#v err=%v", projection.Cursors, err)
	}
}

func TestStoreSeedsDemoNodeCursorFromItsProjection(t *testing.T) {
	store := NewStore()
	err := store.ApplyEvent(context.Background(), Event{EventID: "evt-old-demo", EventType: "run.updated", OrganizationID: "org-demo", NodeID: "node-01", Sequence: 6})
	if err == nil {
		t.Fatal("expected event behind seeded cursor 7 to be rejected")
	}
}
