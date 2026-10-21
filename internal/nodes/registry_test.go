package nodes

import "testing"

func TestRegistryTracksHelloAndHeartbeat(t *testing.T) {
	registry := NewRegistry()
	registry.Hello(Node{ID: "node-1", OrganizationID: "org-1", Capabilities: []string{"run.start"}})
	registry.Heartbeat("node-1", "ready")

	node, ok := registry.Get("node-1")
	if !ok || node.Status != "ready" || node.OrganizationID != "org-1" {
		t.Fatalf("unexpected node state: %#v, found=%v", node, ok)
	}
}

func TestRegistryIgnoresHeartbeatFromUnknownNode(t *testing.T) {
	registry := NewRegistry()
	registry.Heartbeat("unknown", "ready")
	if _, ok := registry.Get("unknown"); ok {
		t.Fatal("unknown heartbeat must not create a node")
	}
}

func TestRegistryAppliesNodeEventsOnceAndInOrder(t *testing.T) {
	registry := NewRegistry()
	if !registry.Apply(Event{NodeID: "node-1", OrganizationID: "org-1", Method: "harness.status", Sequence: 1, Harnesses: []byte(`[{"id":"codex"}]`)}) {
		t.Fatal("first event must apply")
	}
	if registry.Apply(Event{NodeID: "node-1", Method: "harness.status", Sequence: 1, Harnesses: []byte(`[]`)}) {
		t.Fatal("a duplicate sequence must not apply")
	}
	registry.Apply(Event{NodeID: "node-1", Method: "approval.requested", Sequence: 2, RequestID: "q1", Params: []byte(`{"runId":"r1","requestId":"q1"}`)})
	registry.Apply(Event{NodeID: "node-1", Method: "run.updated", Sequence: 3, RunID: "r1", State: "waiting", Params: []byte(`{"runId":"r1","state":"waiting"}`)})
	node, _ := registry.Get("node-1")
	if string(node.Harnesses) != `[{"id":"codex"}]` || len(node.PendingApprovals) != 1 || node.LastSequence != 3 || node.OrganizationID != "org-1" {
		t.Fatalf("unexpected node: %#v", node)
	}
	registry.Apply(Event{NodeID: "node-1", Method: "run.updated", Sequence: 4, RunID: "r1", State: "running", Params: []byte(`{"runId":"r1","state":"running"}`)})
	node, _ = registry.Get("node-1")
	if len(node.PendingApprovals) != 0 || len(node.Runs) != 1 {
		t.Fatalf("the approval must go once the run goes on: %#v", node)
	}

	// Hello again keeps what the node reported.
	registry.Hello(Node{ID: "node-1", OrganizationID: "org-1"})
	node, _ = registry.Get("node-1")
	if node.LastSequence != 4 || len(node.Harnesses) == 0 {
		t.Fatalf("hello must keep reported state: %#v", node)
	}
}
