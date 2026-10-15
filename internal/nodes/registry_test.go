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
