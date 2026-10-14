package auth

import (
	"errors"
	"net/http/httptest"
	"testing"
)

func cfg() *Config {
	return &Config{
		Enabled: true,
		Keys: map[string]Principal{
			"key-a":     {Name: "svc-a", Tenant: "tenant-a"},
			"key-b":     {Name: "svc-b", Tenant: "tenant-b"},
			"key-admin": {Name: "ops", Superuser: true},
		},
		ACLs: []ACL{
			{Principal: "svc-a", Resource: "orders", Operations: []Operation{OpProduce}},
			{Principal: "svc-a", Resource: "audit.*", Operations: []Operation{OpConsume}},
			{Principal: "svc-b", Resource: "*", Operations: []Operation{OpConsume}},
		},
	}
}

func TestAuthenticate(t *testing.T) {
	c := cfg()
	r := httptest.NewRequest("GET", "/", nil)
	if _, err := c.Authenticate(r); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("missing token accepted")
	}
	r.Header.Set("Authorization", "Bearer key-a")
	p, err := c.Authenticate(r)
	if err != nil || p.Name != "svc-a" {
		t.Fatal(p, err)
	}
	r.Header.Set("Authorization", "Bearer key-x")
	if _, err := c.Authenticate(r); err == nil {
		t.Fatal("bad token accepted")
	}
}

func TestProduceDoesNotImplyConsume(t *testing.T) {
	c := cfg()
	a := c.Keys["key-a"]
	if err := c.Authorize(a, OpProduce, "orders", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Authorize(a, OpConsume, "orders", "tenant-a"); !errors.Is(err, ErrForbidden) {
		t.Fatal("produce permission implied consume")
	}
	if err := c.Authorize(a, OpConsume, "audit.logins", "tenant-a"); err != nil {
		t.Fatal("prefix ACL", err)
	}
}

func TestTenantIsolation(t *testing.T) {
	c := cfg()
	b := c.Keys["key-b"]
	// svc-b has a wildcard consume ACL but must not read tenant-a's topics.
	if err := c.Authorize(b, OpConsume, "orders", "tenant-a"); !errors.Is(err, ErrForbidden) {
		t.Fatal("wildcard ACL crossed tenant boundary")
	}
	if err := c.Authorize(b, OpConsume, "payments", "tenant-b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Authorize(c.Keys["key-admin"], OpAdmin, "orders", "tenant-a"); err != nil {
		t.Fatal("superuser")
	}
}

func TestDisabled(t *testing.T) {
	var c Config
	p, err := c.Authenticate(httptest.NewRequest("GET", "/", nil))
	if err != nil || c.Authorize(p, OpAdmin, "x", "y") != nil {
		t.Fatal("disabled auth should allow")
	}
}
