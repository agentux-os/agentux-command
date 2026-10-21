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

func eventParams(sequence int64) Params {
	return Params{
		ProtocolVersion: "v1", OrganizationID: "org-1", NodeID: "node-1", MessageID: "msg-1", OccurredAt: "2026-10-21T12:00:00Z",
		EventID: "node-1-1", Sequence: sequence,
	}
}

func TestValidateAcceptsNodeEvents(t *testing.T) {
	status := eventParams(1)
	status.Harnesses = []byte(`[{"id":"codex","installed":true}]`)
	limits := eventParams(2)
	limits.Limits = []byte(`[]`)
	for method, params := range map[string]Params{"harness.status": status, "harness.limits_changed": limits, "run.updated": eventParams(3)} {
		if err := Validate(Message{JSONRPC: "2.0", Method: method, Params: params}); err != nil {
			t.Fatalf("expected valid %s: %v", method, err)
		}
	}
}

func TestValidateRejectsEventsWithoutIdentityOrPayload(t *testing.T) {
	if err := Validate(Message{JSONRPC: "2.0", Method: "run.updated", Params: eventParams(0)}); err == nil {
		t.Fatal("expected an event without a sequence to be rejected")
	}
	if err := Validate(Message{JSONRPC: "2.0", Method: "harness.status", Params: eventParams(1)}); err == nil {
		t.Fatal("expected harness.status without harnesses to be rejected")
	}
}
