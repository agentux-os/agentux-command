package protocol

import (
	"fmt"
	"time"
)

type Message struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  Params `json:"params"`
}

type Params struct {
	ProtocolVersion           string   `json:"protocolVersion"`
	OrganizationID            string   `json:"organizationId"`
	NodeID                    string   `json:"nodeId"`
	MessageID                 string   `json:"messageId"`
	OccurredAt                string   `json:"occurredAt"`
	CommandID                 string   `json:"commandId,omitempty"`
	EventID                   string   `json:"eventId,omitempty"`
	Sequence                  int64    `json:"sequence,omitempty"`
	MissionID                 string   `json:"missionId,omitempty"`
	RunID                     string   `json:"runId,omitempty"`
	Capabilities              []string `json:"capabilities,omitempty"`
	AgentUXVersion            string   `json:"agentuxVersion,omitempty"`
	SupportedProtocolVersions []string `json:"supportedProtocolVersions,omitempty"`
	Status                    string   `json:"status,omitempty"`
	Capacity                  any      `json:"capacity,omitempty"`
	LastAcknowledgedSequence  int64    `json:"lastAcknowledgedSequence,omitempty"`
	ProviderToken             string   `json:"providerToken,omitempty"`
}

func Validate(message Message) error {
	if message.JSONRPC != "2.0" {
		return fmt.Errorf("jsonrpc must be 2.0")
	}
	if message.Method == "" {
		return fmt.Errorf("method is required")
	}
	base := map[string]string{
		"protocolVersion": message.Params.ProtocolVersion,
		"organizationId":  message.Params.OrganizationID,
		"nodeId":          message.Params.NodeID,
		"messageId":       message.Params.MessageID,
		"occurredAt":      message.Params.OccurredAt,
	}
	for name, value := range base {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if message.Params.ProtocolVersion != "v1" {
		return fmt.Errorf("unsupported protocol version %q", message.Params.ProtocolVersion)
	}
	if _, err := time.Parse(time.RFC3339, message.Params.OccurredAt); err != nil {
		return fmt.Errorf("occurredAt must be RFC3339: %w", err)
	}
	if message.Params.ProviderToken != "" {
		return fmt.Errorf("provider credentials are not protocol payloads")
	}

	switch message.Method {
	case "node.hello":
		if len(message.Params.Capabilities) == 0 || message.Params.AgentUXVersion == "" || len(message.Params.SupportedProtocolVersions) == 0 {
			return fmt.Errorf("node.hello requires capabilities, agentuxVersion and supportedProtocolVersions")
		}
	case "node.heartbeat":
		if message.Params.Status == "" || message.Params.Capacity == nil {
			return fmt.Errorf("node.heartbeat requires status and capacity")
		}
	case "node.resume":
		if message.Params.LastAcknowledgedSequence < 0 {
			return fmt.Errorf("lastAcknowledgedSequence cannot be negative")
		}
	case "run.start":
		for name, value := range map[string]string{"commandId": message.Params.CommandID, "missionId": message.Params.MissionID, "runId": message.Params.RunID} {
			if value == "" {
				return fmt.Errorf("run.start requires %s", name)
			}
		}
	}
	return nil
}
