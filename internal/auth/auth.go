// Package auth implements bearer-token authentication, topic ACLs and
// tenant isolation.
//
// Scope and limits (stated plainly): credentials are static API keys loaded
// from a JSON file, compared in constant time. There is no key rotation
// service, no OAuth/mTLS identity, and no secret store integration. TLS is
// supported by the servers but certificates are supplied by the operator.
// Production systems would integrate with an identity provider and a
// secrets manager; the authorization model below is the part that carries
// over.
package auth

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Operation is an action on a resource.
type Operation string

const (
	OpProduce  Operation = "produce"
	OpConsume  Operation = "consume"
	OpDescribe Operation = "describe"
	OpAdmin    Operation = "admin"
	// OpReplicate is used by brokers for inter-broker replication.
	OpReplicate Operation = "replicate"
)

// Principal is an authenticated identity.
type Principal struct {
	Name      string `json:"name"`
	Tenant    string `json:"tenant"`
	Superuser bool   `json:"superuser"`
}

// Anonymous is used when authentication is disabled.
var Anonymous = Principal{Name: "anonymous", Superuser: true}

// ACL grants operations on topics matching Resource. A trailing "*" is a
// prefix match ("orders.*"); "*" alone matches everything.
type ACL struct {
	Principal  string      `json:"principal"`
	Resource   string      `json:"resource"`
	Operations []Operation `json:"operations"`
}

// Config is the security configuration.
type Config struct {
	Enabled bool `json:"enabled"`
	// Keys maps API tokens to principals.
	Keys map[string]Principal `json:"keys"`
	ACLs []ACL                `json:"acls"`
}

// Errors.
var (
	ErrUnauthenticated = errors.New("missing or invalid credentials")
	ErrForbidden       = errors.New("operation not permitted")
)

// Load reads a config file. An empty path returns a disabled config.
func Load(path string) (*Config, error) {
	if path == "" {
		return &Config{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &c, nil
}

// Authenticate resolves the bearer token on r.
func (c *Config) Authenticate(r *http.Request) (Principal, error) {
	if c == nil || !c.Enabled {
		return Anonymous, nil
	}
	h := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || tok == "" {
		return Principal{}, ErrUnauthenticated
	}
	// Compare against every key in constant time so response timing does
	// not reveal how much of a guessed key matched.
	var found Principal
	match := 0
	for k, p := range c.Keys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(tok)) == 1 {
			found = p
			match = 1
		}
	}
	if match == 0 {
		return Principal{}, ErrUnauthenticated
	}
	return found, nil
}

// Authorize decides whether p may perform op on topic owned by topicTenant.
//
// Tenant isolation is the first check: a principal may only touch topics of
// its own tenant unless an ACL explicitly names the topic. Producing to a
// topic never implies permission to consume from it, and vice versa.
func (c *Config) Authorize(p Principal, op Operation, topic, topicTenant string) error {
	if c == nil || !c.Enabled || p.Superuser {
		return nil
	}
	sameTenant := topicTenant == "" || topicTenant == p.Tenant
	for _, a := range c.ACLs {
		if a.Principal != p.Name && a.Principal != "*" {
			continue
		}
		if !matchResource(a.Resource, topic) {
			continue
		}
		explicit := a.Resource == topic
		if !sameTenant && !explicit {
			continue
		}
		for _, o := range a.Operations {
			if o == op {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: %s may not %s %q", ErrForbidden, p.Name, op, topic)
}

func matchResource(pattern, topic string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(topic, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == topic
}
