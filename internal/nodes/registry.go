package nodes

import "sync"

type Node struct {
	ID             string   `json:"nodeId"`
	OrganizationID string   `json:"organizationId"`
	Capabilities   []string `json:"capabilities"`
	Status         string   `json:"status"`
}

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
	node.Status = "online"
	r.nodes[node.ID] = node
}

func (r *Registry) Heartbeat(id, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	node := r.nodes[id]
	node.Status = status
	r.nodes[id] = node
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
	return result
}
