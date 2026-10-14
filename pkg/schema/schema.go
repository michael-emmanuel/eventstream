// Package schema is a minimal schema-versioning abstraction for event
// payloads. It deliberately does not require a schema registry service:
// producers stamp each record with event_type and schema_version headers,
// and consumers use a Registry (in-memory here; a remote registry would
// implement the same interface) to validate, check compatibility, and
// upgrade older payloads.
//
// The broker never inspects payloads. Schema evolution is a contract
// between producers and consumers, enforced at the edges, because the log
// keeps old events for as long as retention allows: any consumer may read
// any historical version during replay.
package schema

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// Header keys carried on every record.
const (
	HeaderEventType     = "event_type"
	HeaderSchemaVersion = "schema_version"
)

// Compatibility modes.
type Compatibility string

const (
	// Backward: consumers using the NEW schema can read data written with
	// the OLD schema. Safe to upgrade consumers first. Adding a field
	// requires a default; removing a field is allowed.
	Backward Compatibility = "backward"
	// Forward: consumers using the OLD schema can read data written with
	// the NEW schema. Safe to upgrade producers first. Removing a required
	// field is forbidden; adding fields is allowed (old readers ignore them).
	Forward Compatibility = "forward"
	// Full: both directions. Required for replay across arbitrary versions
	// with independently deployed producers and consumers.
	Full Compatibility = "full"
	// None: no checks.
	None Compatibility = "none"
)

// Field describes one payload field.
type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // string | number | bool | object | array
	Required bool   `json:"required"`
	Default  any    `json:"default,omitempty"`
}

// Schema is one version of an event type.
type Schema struct {
	EventType string  `json:"event_type"`
	Version   int     `json:"version"`
	Fields    []Field `json:"fields"`
}

func (s Schema) field(name string) (Field, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// Registry stores schema versions per event type.
type Registry interface {
	Register(s Schema) (int, error)
	Get(eventType string, version int) (Schema, bool)
	Latest(eventType string) (Schema, bool)
}

// Errors.
var (
	ErrIncompatible  = errors.New("schema: incompatible change")
	ErrUnknownSchema = errors.New("schema: unknown event type or version")
	ErrInvalid       = errors.New("schema: payload does not match schema")
)

// MemoryRegistry is an in-process Registry with compatibility enforcement.
type MemoryRegistry struct {
	mode     Compatibility
	mu       sync.RWMutex
	versions map[string][]Schema
}

// NewMemoryRegistry creates a registry enforcing mode on every Register.
func NewMemoryRegistry(mode Compatibility) *MemoryRegistry {
	return &MemoryRegistry{mode: mode, versions: map[string][]Schema{}}
}

// Register adds the next version of an event type after checking it is
// compatible with the latest one. It returns the assigned version.
func (r *MemoryRegistry) Register(s Schema) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vs := r.versions[s.EventType]
	if n := len(vs); n > 0 {
		if err := Check(vs[n-1], s, r.mode); err != nil {
			return 0, err
		}
	}
	s.Version = len(vs) + 1
	r.versions[s.EventType] = append(vs, s)
	return s.Version, nil
}

// Get returns a specific version.
func (r *MemoryRegistry) Get(eventType string, version int) (Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vs := r.versions[eventType]
	if version < 1 || version > len(vs) {
		return Schema{}, false
	}
	return vs[version-1], true
}

// Latest returns the newest version.
func (r *MemoryRegistry) Latest(eventType string) (Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vs := r.versions[eventType]
	if len(vs) == 0 {
		return Schema{}, false
	}
	return vs[len(vs)-1], true
}

// Check reports whether next may follow prev under mode.
func Check(prev, next Schema, mode Compatibility) error {
	var problems []string
	backward := func() {
		for _, f := range next.Fields {
			old, ok := prev.field(f.Name)
			if !ok && f.Required && f.Default == nil {
				problems = append(problems, fmt.Sprintf("new required field %q has no default (old data lacks it)", f.Name))
			}
			if ok && old.Type != f.Type {
				problems = append(problems, fmt.Sprintf("field %q changed type %s -> %s", f.Name, old.Type, f.Type))
			}
		}
	}
	forward := func() {
		for _, f := range prev.Fields {
			nf, ok := next.field(f.Name)
			if f.Required && !ok {
				problems = append(problems, fmt.Sprintf("required field %q removed (old readers need it)", f.Name))
			}
			if ok && nf.Type != f.Type {
				problems = append(problems, fmt.Sprintf("field %q changed type %s -> %s", f.Name, f.Type, nf.Type))
			}
		}
	}
	switch mode {
	case Backward:
		backward()
	case Forward:
		forward()
	case Full:
		backward()
		forward()
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("%w (%s): %v", ErrIncompatible, mode, problems)
	}
	return nil
}

// Validate checks a decoded JSON payload against a schema.
func Validate(s Schema, payload map[string]any) error {
	for _, f := range s.Fields {
		v, ok := payload[f.Name]
		if !ok {
			if f.Required {
				return fmt.Errorf("%w: missing %q", ErrInvalid, f.Name)
			}
			continue
		}
		if !typeMatches(f.Type, v) {
			return fmt.Errorf("%w: %q is not %s", ErrInvalid, f.Name, f.Type)
		}
	}
	return nil
}

func typeMatches(t string, v any) bool {
	switch t {
	case "string":
		_, ok := v.(string)
		return ok
	case "number":
		_, ok := v.(float64)
		return ok
	case "bool":
		_, ok := v.(bool)
		return ok
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	}
	return true
}

// Upgrade rewrites a payload written with an older version into the shape
// of target by filling defaults for fields the old version lacked. Fields
// unknown to target are kept (forward-compatible readers ignore them).
// This is how a consumer on schema v3 processes a v1 event during replay.
func Upgrade(payload map[string]any, target Schema) map[string]any {
	out := make(map[string]any, len(payload)+len(target.Fields))
	for k, v := range payload {
		out[k] = v
	}
	for _, f := range target.Fields {
		if _, ok := out[f.Name]; !ok && f.Default != nil {
			out[f.Name] = f.Default
		}
	}
	return out
}

// Resolve finds the schema for a record from its headers.
func Resolve(r Registry, headers map[string]string) (Schema, error) {
	et := headers[HeaderEventType]
	v, err := strconv.Atoi(headers[HeaderSchemaVersion])
	if et == "" || err != nil {
		return Schema{}, fmt.Errorf("%w: headers %s=%q %s=%q", ErrUnknownSchema,
			HeaderEventType, et, HeaderSchemaVersion, headers[HeaderSchemaVersion])
	}
	s, ok := r.Get(et, v)
	if !ok {
		// A consumer older than the producer sees a version it does not
		// know. Under forward compatibility it can still read the event
		// with the latest schema it has.
		if latest, ok := r.Latest(et); ok && v > latest.Version {
			return latest, nil
		}
		return Schema{}, fmt.Errorf("%w: %s v%d", ErrUnknownSchema, et, v)
	}
	return s, nil
}
