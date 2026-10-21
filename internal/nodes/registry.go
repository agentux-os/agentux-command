package nodes

import (
	"encoding/json"
	"sort"
	"sync"
)

// Node is what Command knows of one AgentUX node: what it said in
// node.hello and heartbeats, and the latest of its agent, limit, run and
// approval events.
type Node struct {
	ID             string   `json:"nodeId"`
	OrganizationID string   `json:"organizationId"`
	Capabilities   []string `json:"capabilities"`
	Status         string   `json:"status"`
	AgentUXVersion string   `json:"agentuxVersion,omitempty"`
	LastSeenAt     string   `json:"lastSeenAt,omitempty"`
	Capacity       any      `json:"capacity,omitempty"`
	// The node's agent CLIs and usage limits, as its last harness.status and
	// harness.limits_changed reported them.
	Harnesses json.RawMessage `json:"harnesses,omitempty"`
	Limits    json.RawMessage `json:"limits,omitempty"`
	// The latest event of each run, and approvals still waiting, by id.
	Runs             map[string]json.RawMessage `json:"runs,omitempty"`
	PendingApprovals map[string]json.RawMessage `json:"pendingApprovals,omitempty"`
	LastSequence     int64                      `json:"lastSequence"`
}

// Event is one node event as the registry applies it.
type Event struct {
	NodeID         string
	OrganizationID string
	Method         string
	Sequence       int64
	OccurredAt     string
	RunID          string
	RequestID      string
	State          string
	Harnesses      json.RawMessage
	Limits         json.RawMessage
	// The whole params object, kept for runs and approvals.
	Params json.RawMessage
}

// maxRuns bounds the runs kept per node; the oldest finished ones go first.
const maxRuns = 200

type Registry struct {
	mu    sync.RWMutex
	nodes map[string]Node
}

func NewRegistry() *Registry {
	return &Registry{nodes: make(map[string]Node)}
}

func (r *Registry) Hello(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// A node saying hello again keeps what it reported before.
	if known, ok := r.nodes[node.ID]; ok {
		node.Harnesses, node.Limits = known.Harnesses, known.Limits
		node.Runs, node.PendingApprovals = known.Runs, known.PendingApprovals
		node.LastSequence = known.LastSequence
	}
	node.Status = "online"
	r.nodes[node.ID] = node
}

func (r *Registry) Heartbeat(id, status string) {
	r.HeartbeatWith(id, status, nil, "")
}

// HeartbeatWith records a heartbeat with the node's capacity and time.
func (r *Registry) HeartbeatWith(id, status string, capacity any, at string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[id]
	if !ok {
		return
	}
	node.Status = status
	if capacity != nil {
		node.Capacity = capacity
	}
	if at != "" {
		node.LastSeenAt = at
	}
	r.nodes[id] = node
}

// Apply records a node event. Events are at least once: one whose sequence
// is not above the node's last is a duplicate and changes nothing. Events
// from a node Command does not know yet (Command restarted) register it.
// It returns whether the event was new.
func (r *Registry) Apply(event Event) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[event.NodeID]
	if !ok {
		node = Node{ID: event.NodeID, OrganizationID: event.OrganizationID, Status: "online"}
	}
	if event.Sequence <= node.LastSequence {
		return false
	}
	node.LastSequence = event.Sequence
	if event.OccurredAt != "" {
		node.LastSeenAt = event.OccurredAt
	}
	switch event.Method {
	case "harness.status":
		node.Harnesses = event.Harnesses
	case "harness.limits_changed":
		node.Limits = event.Limits
	case "approval.requested":
		if event.RequestID != "" {
			node.PendingApprovals = withEntry(node.PendingApprovals, event.RequestID, event.Params)
		}
	case "run.accepted", "run.started", "run.updated", "run.completed", "run.failed":
		if event.RunID != "" {
			node.Runs = withEntry(node.Runs, event.RunID, event.Params)
			if len(node.Runs) > maxRuns {
				trimRuns(node.Runs)
			}
			// A run that is not waiting has no approval pending any more.
			if event.State != "waiting" {
				for id, raw := range node.PendingApprovals {
					var approval struct {
						RunID string `json:"runId"`
					}
					if json.Unmarshal(raw, &approval) == nil && approval.RunID == event.RunID {
						delete(node.PendingApprovals, id)
					}
				}
			}
		}
	}
	r.nodes[event.NodeID] = node
	return true
}

func withEntry(entries map[string]json.RawMessage, id string, raw json.RawMessage) map[string]json.RawMessage {
	if entries == nil {
		entries = make(map[string]json.RawMessage)
	}
	entries[id] = raw
	return entries
}

// trimRuns drops finished runs, oldest started first, down to maxRuns.
func trimRuns(runs map[string]json.RawMessage) {
	type finished struct{ id, startedAt string }
	var done []finished
	for id, raw := range runs {
		var run struct {
			State     string `json:"state"`
			StartedAt string `json:"startedAt"`
		}
		if json.Unmarshal(raw, &run) == nil && (run.State == "done" || run.State == "failed" || run.State == "cancelled") {
			done = append(done, finished{id, run.StartedAt})
		}
	}
	sort.Slice(done, func(i, j int) bool { return done[i].startedAt < done[j].startedAt })
	for _, run := range done {
		if len(runs) <= maxRuns {
			return
		}
		delete(runs, run.id)
	}
}

func (r *Registry) Get(id string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodes[id]
	return node, ok
}

func (r *Registry) List() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
