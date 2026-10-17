package office

import "testing"

func TestStoreDeduplicatesEventsAndKeepsLastSequence(t *testing.T) {
	store := NewStore()
	event := Event{EventID: "evt-1", EventType: "run.updated", OrganizationID: "org-demo", NodeID: "node-01", Sequence: 8}
	if err := store.ApplyEvent(event); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyEvent(event); err != nil {
		t.Fatal(err)
	}
	projection, ok := store.GetProjection("org-demo")
	if !ok || projection.LastEventID != "evt-1" || projection.LastSequence != 8 {
		t.Fatalf("expected deduplicated replay metadata, got %#v", projection)
	}
}

func TestStoreReturnsOriginalApprovalForIdempotentRetry(t *testing.T) {
	store := NewStore()
	request := ResolutionRequest{Decision: "approved", Reason: "reviewed", IdempotencyKey: "idem-01"}
	first, err := store.ResolveApproval("approval-01", request)
	if err != nil || first.IdempotentReplay {
		t.Fatalf("expected first decision, response=%#v err=%v", first, err)
	}
	replay, err := store.ResolveApproval("approval-01", request)
	if err != nil || !replay.IdempotentReplay || replay.Decision != "approved" {
		t.Fatalf("expected replayed decision, response=%#v err=%v", replay, err)
	}
}

func TestStoreKeepsNodeSequencesPartitionedByOrganization(t *testing.T) {
	store := NewStore()
	for _, organizationID := range []string{"org-a", "org-b"} {
		err := store.ApplyEvent(Event{EventID: "evt-" + organizationID, EventType: "run.started", OrganizationID: organizationID, NodeID: "node-01", Sequence: 1})
		if err != nil {
			t.Fatalf("apply event for %s: %v", organizationID, err)
		}
	}
	for _, organizationID := range []string{"org-a", "org-b"} {
		projection, ok := store.GetProjection(organizationID)
		if !ok || projection.LastSequence != 1 || projection.OrganizationID != organizationID {
			t.Fatalf("expected isolated projection for %s, got %#v", organizationID, projection)
		}
	}
}
