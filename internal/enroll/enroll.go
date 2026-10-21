// Package enroll lets AgentUX nodes join one Command server, and only that
// one.
//
// Every Command server has an instance id. Operators issue enrollment
// tokens that name it (`auxe1_<instanceId>_<secret>`), are single use and
// expire. A node redeems one token at this server's /v1/enroll and gets a
// node credential of its own (`auxn1_<secret>`), bound to this instance, its
// organization and a node id unique here. Two Command servers in the same
// institution never accept each other's tokens or credentials, so neither
// can take the other's nodes; nodes pin the instance id they enrolled with.
//
// Only hashes of tokens and credentials are stored.
package enroll

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	TokenPrefix  = "auxe1_"
	SecretPrefix = "auxn1_"
)

var (
	ErrInvalidToken     = errors.New("the enrollment token is not valid here")
	ErrOtherInstance    = errors.New("the enrollment token belongs to another Command server")
	ErrExpiredToken     = errors.New("the enrollment token has expired")
	ErrUsedToken        = errors.New("the enrollment token was already used")
	ErrUnknownNode      = errors.New("unknown node")
	nodeIDPattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
	instanceIDPattern   = regexp.MustCompile(`^cmd-[0-9a-f]{16}$`)
	organizationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
)

// TokenRequest is what an operator asks for.
type TokenRequest struct {
	OrganizationID  string        `json:"organizationId"`
	NodeName        string        `json:"nodeName,omitempty"`
	Count           int           `json:"count"`
	TTL             time.Duration `json:"-"`
	RequireApproval bool          `json:"requireApproval"`
	CreatedBy       string        `json:"-"`
}

// IssuedToken is shown once, when it is created.
type IssuedToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
	NodeName  string    `json:"nodeName,omitempty"`
}

// Credential is a node's membership of this Command server.
type Credential struct {
	NodeID         string     `json:"nodeId"`
	OrganizationID string     `json:"organizationId"`
	Hostname       string     `json:"hostname,omitempty"`
	Approved       bool       `json:"approved"`
	EnrolledAt     time.Time  `json:"enrolledAt"`
	RevokedAt      *time.Time `json:"revokedAt,omitempty"`
}

// Active means the node may send and receive.
func (c Credential) Active() bool { return c.Approved && c.RevokedAt == nil }

// Redemption is what a node gets for a valid token.
type Redemption struct {
	Credential Credential
	// The node's secret, shown once.
	Secret string
}

// tokenRecord is what a store keeps for a token.
type tokenRecord struct {
	OrganizationID  string
	NodeName        string
	RequireApproval bool
	ExpiresAt       time.Time
	Used            bool
}

// Store keeps the instance id, tokens and credentials.
type Store interface {
	InstanceID(ctx context.Context) (string, error)
	IssueTokens(ctx context.Context, request TokenRequest, now time.Time) ([]IssuedToken, error)
	// Redeem checks the token, takes a node id and stores the credential,
	// all at once: a token is never redeemed twice.
	Redeem(ctx context.Context, token, nodeName, hostname string, now time.Time) (Redemption, error)
	Authenticate(ctx context.Context, nodeID, secret string) (Credential, error)
	Credentials(ctx context.Context) ([]Credential, error)
	Approve(ctx context.Context, nodeID string, now time.Time) error
	Revoke(ctx context.Context, nodeID string, now time.Time) error
}

// NewInstanceID is `cmd-` and 16 hex digits.
func NewInstanceID() string { return "cmd-" + randomHex(8) }

// ValidInstanceID checks the shape of an instance id.
func ValidInstanceID(id string) bool { return instanceIDPattern.MatchString(id) }

// TokenInstance is the instance id a token names.
func TokenInstance(token string) (string, bool) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(token), TokenPrefix)
	if !ok {
		return "", false
	}
	instance, secret, ok := strings.Cut(rest, "_")
	if !ok || !ValidInstanceID(instance) || len(secret) != 64 {
		return "", false
	}
	return instance, true
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)
}

func hash(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func newToken(instance string) string { return TokenPrefix + instance + "_" + randomHex(32) }
func newSecret() string               { return SecretPrefix + randomHex(32) }

// validateRequest checks and fills a token request.
func validateRequest(request *TokenRequest) error {
	if !organizationPattern.MatchString(request.OrganizationID) {
		return fmt.Errorf("organizationId must be letters, digits, '.', '_' or '-'")
	}
	request.NodeName = strings.ToLower(strings.TrimSpace(request.NodeName))
	if request.NodeName != "" && !nodeIDPattern.MatchString(request.NodeName) {
		return fmt.Errorf("nodeName must be lowercase letters, digits, '.', '_' or '-'")
	}
	if request.Count < 1 {
		request.Count = 1
	}
	if request.Count > 500 {
		return fmt.Errorf("at most 500 tokens at a time")
	}
	if request.NodeName != "" && request.Count > 1 {
		return fmt.Errorf("a token for a named node is issued alone")
	}
	if request.TTL <= 0 {
		request.TTL = 24 * time.Hour
	}
	if request.TTL > 30*24*time.Hour {
		return fmt.Errorf("tokens live at most 30 days")
	}
	return nil
}

// nodeID picks this node's id: the token's name, else the node's own name
// or host name, made unique with a suffix when taken.
func nodeID(tokenName, nodeName, hostname string, taken func(string) bool) (string, error) {
	base := tokenName
	if base == "" {
		base = clean(nodeName)
	}
	if base == "" {
		base = clean("node-" + hostname)
	}
	if base == "" || base == "node-" {
		base = "node-" + randomHex(3)
	}
	if tokenName != "" {
		// A named token is for that node only.
		if taken(base) {
			return "", fmt.Errorf("node %q is already enrolled; revoke it first to enroll it again", base)
		}
		return base, nil
	}
	if !taken(base) {
		return base, nil
	}
	for i := 2; i < 1000; i++ {
		candidate := fmt.Sprintf("%s-%d", base, i)
		if !taken(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("no free node id for %q", base)
}

func clean(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-.")
	if len(out) > 63 {
		out = out[:63]
	}
	if !nodeIDPattern.MatchString(out) {
		return ""
	}
	return out
}
