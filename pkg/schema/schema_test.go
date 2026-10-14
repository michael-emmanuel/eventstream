package schema

import (
	"errors"
	"testing"
)

var v1 = Schema{EventType: "order.created", Fields: []Field{
	{Name: "order_id", Type: "number", Required: true},
	{Name: "amount", Type: "number", Required: true},
}}

func TestBackwardAllowsDefaultedAddition(t *testing.T) {
	r := NewMemoryRegistry(Backward)
	r.Register(v1)
	v2 := Schema{EventType: "order.created", Fields: append(v1.Fields, Field{Name: "currency", Type: "string", Required: true, Default: "USD"})}
	if ver, err := r.Register(v2); err != nil || ver != 2 {
		t.Fatal(ver, err)
	}
	// Replay: a v2 consumer reads a v1 event.
	old := map[string]any{"order_id": 1.0, "amount": 10.0}
	up := Upgrade(old, v2)
	if up["currency"] != "USD" || Validate(v2, up) != nil {
		t.Fatalf("upgrade failed: %v", up)
	}
}

func TestBackwardRejectsRequiredWithoutDefault(t *testing.T) {
	r := NewMemoryRegistry(Backward)
	r.Register(v1)
	bad := Schema{EventType: "order.created", Fields: append(v1.Fields, Field{Name: "region", Type: "string", Required: true})}
	if _, err := r.Register(bad); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("err=%v", err)
	}
}

func TestForwardRejectsRemovingRequired(t *testing.T) {
	r := NewMemoryRegistry(Forward)
	r.Register(v1)
	bad := Schema{EventType: "order.created", Fields: v1.Fields[:1]}
	if _, err := r.Register(bad); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("err=%v", err)
	}
}

func TestTypeChangeIsAlwaysIncompatible(t *testing.T) {
	bad := Schema{EventType: "order.created", Fields: []Field{{Name: "order_id", Type: "string", Required: true}, v1.Fields[1]}}
	for _, m := range []Compatibility{Backward, Forward, Full} {
		if err := Check(v1, bad, m); err == nil {
			t.Fatalf("%s accepted a type change", m)
		}
	}
}

func TestResolveNewerVersionFallsBackToLatest(t *testing.T) {
	r := NewMemoryRegistry(Full)
	r.Register(v1)
	s, err := Resolve(r, map[string]string{HeaderEventType: "order.created", HeaderSchemaVersion: "7"})
	if err != nil || s.Version != 1 {
		t.Fatalf("%v %v", s, err)
	}
	if _, err := Resolve(r, map[string]string{}); !errors.Is(err, ErrUnknownSchema) {
		t.Fatal(err)
	}
}
