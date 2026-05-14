package gotools

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ErrInvalidUUID is returned by ParseUUID when the input is empty, malformed,
// or the nil UUID. Callers that need domain- or transport-specific error types
// should wrap this sentinel.
var ErrInvalidUUID = errors.New("invalid uuid")

// SafeUUID returns s if it parses as a non-nil UUID, else "".
func SafeUUID(s string) string {
	if s == "" {
		return ""
	}

	uid, err := uuid.Parse(s)
	if err != nil || uid == uuid.Nil {
		return ""
	}

	return s
}

// SafeUUIDPtr returns s if *s parses as a non-nil UUID, else nil.
func SafeUUIDPtr(s *string) *string {
	if s == nil {
		return nil
	}

	uid, err := uuid.Parse(*s)
	if err != nil || uid == uuid.Nil {
		return nil
	}

	return s
}

// UUIDFromString parses s into a uuid.UUID. Empty string returns uuid.Nil with
// no error so callers can treat empty inputs as "unset".
func UUIDFromString(s string) (uuid.UUID, error) {
	if s == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(s)
}

// ParseUUID parses req into a non-nil uuid.UUID, returning ErrInvalidUUID on
// failure or when the input is the nil UUID.
func ParseUUID(req string) (uuid.UUID, error) {
	id, err := uuid.Parse(req)
	if err != nil {
		return uuid.Nil, ErrInvalidUUID
	}

	if id == uuid.Nil {
		return uuid.Nil, ErrInvalidUUID
	}

	return id, nil
}

// ParseUUIDOrNil returns a *string of the canonicalized UUID, or nil if the
// input is empty / malformed / nil UUID.
func ParseUUIDOrNil(id string) *string {
	if id == "" {
		return nil
	}

	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil {
		return nil
	}

	s := parsed.String()
	return &s
}

// OptionalUUIDString returns s.String() or "" if s is nil.
func OptionalUUIDString(s *uuid.UUID) string {
	if s == nil {
		return ""
	}
	return s.String()
}

// UUIDSet is an unordered set of UUIDs backed by a map.
type UUIDSet map[uuid.UUID]struct{}

// Has reports whether id is present in the set.
func (s UUIDSet) Has(id uuid.UUID) bool {
	_, ok := s[id]
	return ok
}

// Empty reports whether the set contains no elements.
func (s UUIDSet) Empty() bool {
	return len(s) == 0
}

// Add inserts id into the set.
func (s UUIDSet) Add(id uuid.UUID) {
	s[id] = struct{}{}
}

// Delete removes id from the set. No-op if not present.
func (s UUIDSet) Delete(id uuid.UUID) {
	delete(s, id)
}

// Len returns the number of elements in the set.
func (s UUIDSet) Len() int {
	return len(s)
}

// Clone returns a shallow copy of the set.
func (s UUIDSet) Clone() UUIDSet {
	c := make(UUIDSet, len(s))
	for k, v := range s {
		c[k] = v
	}
	return c
}

// Merge adds all elements from other into s (in-place union).
func (s UUIDSet) Merge(other UUIDSet) {
	for k := range other {
		s[k] = struct{}{}
	}
}

// Intersect returns a new set containing only elements present in both s and other.
func (s UUIDSet) Intersect(other UUIDSet) UUIDSet {
	result := make(UUIDSet)
	for k := range s {
		if other.Has(k) {
			result[k] = struct{}{}
		}
	}
	return result
}

// Difference returns a new set with elements in s that are not in other.
func (s UUIDSet) Difference(other UUIDSet) UUIDSet {
	result := make(UUIDSet)
	for k := range s {
		if !other.Has(k) {
			result[k] = struct{}{}
		}
	}
	return result
}

// ToSlice returns the set's elements as an unordered slice.
func (s UUIDSet) ToSlice() []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(s))
	for k := range s {
		ids = append(ids, k)
	}
	return ids
}

// ToUUIDSet parses a slice of UUID strings into a UUIDSet,
// silently ignoring any malformed entries.
func ToUUIDSet(ids []string) UUIDSet {
	set := make(UUIDSet, len(ids))
	for _, raw := range ids {
		if id, err := uuid.Parse(raw); err == nil {
			set[id] = struct{}{}
		}
	}
	return set
}

// ParseSafeUUID parses s and returns the UUID, or uuid.Nil for empty,
// malformed or nil-UUID inputs. Use when you want a best-effort parse with
// no error path; pair with ParseUUID when you need to surface the failure.
func ParseSafeUUID(s string) uuid.UUID {
	if s == "" {
		return uuid.Nil
	}
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil {
		return uuid.Nil
	}
	return id
}

// ParseStringsToUUIDs parses every string in ids into a uuid.UUID, returning
// an error wrapping the offending value on the first parse failure. An empty
// input yields a nil slice without error.
func ParseStringsToUUIDs(ids []string) ([]uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]uuid.UUID, 0, len(ids))
	for _, raw := range ids {
		id, err := uuid.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid uuid %q: %w", raw, err)
		}
		out = append(out, id)
	}
	return out, nil
}
