package protocol

import "testing"

func TestValidateAcceptsNodeHello(t *testing.T) {

	message := Message{JSONRPC: "2.0", ID: "hello-1", Method: "node.hello", Params: Params{
		ProtocolVersion: "v1", OrganizationID: "org-1", NodeID: "node-1", MessageID: "msg-1", OccurredAt: "2026-10-15T12:00:00Z",
		Capabilities: []string{"run.start"}, AgentUXVersion: "0.5.0", SupportedProtocolVersions: []string{"v1"},
	}}
	if err := Validate(message); err != nil {
		t.Fatalf("expected valid node.hello: %v", err)
	}
}

func TestValidateRejectsProviderCredential(t *testing.T) {
	message := Message{JSONRPC: "2.0", ID: "start-1", Method: "run.start", Params: Params{
		ProtocolVersion: "v1", OrganizationID: "org-1", NodeID: "node-1", MessageID: "msg-1", OccurredAt: "2026-10-15T12:00:00Z",
		ProviderToken: "must-not-be-here",
	}}
	if err := Validate(message); err == nil {
		t.Fatal("expected provider credential to be rejected")
	}
}

func TestValidateRejectsUnknownMethod(t *testing.T) {
	message := Message{JSONRPC: "2.0", Method: "node.secret", Params: Params{
		ProtocolVersion: "v1", OrganizationID: "org-1", NodeID: "node-1", MessageID: "msg-1", OccurredAt: "2026-10-15T12:00:00Z",
	}}
	if err := Validate(message); err == nil {
		t.Fatal("expected unknown method to be rejected")
	}
}
