package gotools

import (
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// ParseCSVInt64s parses a comma-separated list of int64s. Whitespace around
// each item is trimmed. An empty input yields a nil slice without error.
func ParseCSVInt64s(s string) ([]int64, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// ParseCSVUint64s parses a comma-separated list of uint64s. See ParseCSVInt64s.
func ParseCSVUint64s(s string) ([]uint64, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]uint64, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseUint(strings.TrimSpace(p), 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// ParseCSVUUIDs parses a comma-separated list of canonical UUIDs.
func ParseCSVUUIDs(s string) ([]uuid.UUID, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]uuid.UUID, 0, len(parts))
	for _, p := range parts {
		id, err := uuid.Parse(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// Sqrt32 is a float32-only square root that spares the caller a manual cast.
func Sqrt32(x float32) float32 { return float32(math.Sqrt(float64(x))) }

// Round32 rounds to the nearest integer in float32, half-away-from-zero.
func Round32(x float32) float32 { return float32(math.Round(float64(x))) }
