package enroll

import (
	"context"
	"crypto/subtle"
	"sort"
	"sync"
	"time"
)

// Memory is a Store in memory: the demo server and tests. Its nodes have to
// enroll again after a restart.
type Memory struct {
	mu       sync.Mutex
	instance string
	tokens   map[string]tokenRecord
	nodes    map[string]Credential
	secrets  map[string]string // node id -> secret hash
}

func NewMemory(instance string) *Memory {
	if !ValidInstanceID(instance) {
		instance = NewInstanceID()
	}
	return &Memory{instance: instance, tokens: map[string]tokenRecord{}, nodes: map[string]Credential{}, secrets: map[string]string{}}
}

func (m *Memory) InstanceID(context.Context) (string, error) { return m.instance, nil }

func (m *Memory) IssueTokens(_ context.Context, request TokenRequest, now time.Time) ([]IssuedToken, error) {
	if err := validateRequest(&request); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	issued := make([]IssuedToken, 0, request.Count)
	for i := 0; i < request.Count; i++ {
		token := newToken(m.instance)
		expires := now.Add(request.TTL).UTC()
		m.tokens[hash(token)] = tokenRecord{OrganizationID: request.OrganizationID, NodeName: request.NodeName, RequireApproval: request.RequireApproval, ExpiresAt: expires}
		issued = append(issued, IssuedToken{Token: token, ExpiresAt: expires, NodeName: request.NodeName})
	}
	return issued, nil
}

func (m *Memory) Redeem(_ context.Context, token, nodeName, hostname string, now time.Time) (Redemption, error) {
	instance, ok := TokenInstance(token)
	if !ok {
		return Redemption{}, ErrInvalidToken
	}
	if instance != m.instance {
		return Redemption{}, ErrOtherInstance
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := hash(token)
	record, ok := m.tokens[key]
	switch {
	case !ok:
		return Redemption{}, ErrInvalidToken
	case record.Used:
		return Redemption{}, ErrUsedToken
	case !now.Before(record.ExpiresAt):
		return Redemption{}, ErrExpiredToken
	}
	id, err := nodeID(record.NodeName, nodeName, hostname, func(id string) bool {
		c, ok := m.nodes[id]
		return ok && c.RevokedAt == nil
	})
	if err != nil {
		return Redemption{}, err
	}
	secret := newSecret()
	credential := Credential{NodeID: id, OrganizationID: record.OrganizationID, Hostname: hostname, Approved: !record.RequireApproval, EnrolledAt: now.UTC()}
	record.Used = true
	m.tokens[key] = record
	m.nodes[id] = credential
	m.secrets[id] = hash(secret)
	return Redemption{Credential: credential, Secret: secret}, nil
}

func (m *Memory) Authenticate(_ context.Context, nodeID, secret string) (Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.nodes[nodeID]
	if !ok || subtle.ConstantTimeCompare([]byte(m.secrets[nodeID]), []byte(hash(secret))) != 1 || c.RevokedAt != nil {
		return Credential{}, ErrUnknownNode
	}
	return c, nil
}

func (m *Memory) Credentials(context.Context) ([]Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Credential, 0, len(m.nodes))
	for _, c := range m.nodes {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (m *Memory) Approve(_ context.Context, nodeID string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.nodes[nodeID]
	if !ok || c.RevokedAt != nil {
		return ErrUnknownNode
	}
	c.Approved = true
	m.nodes[nodeID] = c
	return nil
}

func (m *Memory) Revoke(_ context.Context, nodeID string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.nodes[nodeID]
	if !ok {
		return ErrUnknownNode
	}
	at := now.UTC()
	c.RevokedAt = &at
	m.nodes[nodeID] = c
	return nil
}
