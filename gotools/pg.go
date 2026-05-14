package gotools

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
)

// IsPgUniqueViolation reports whether err carries a PostgreSQL
// unique-violation SQLSTATE (23505). Works with any driver whose error type
// exposes SQLState() string (pgx, pq).
func IsPgUniqueViolation(err error) bool {
	return hasPgSQLState(err, pgerrcode.UniqueViolation)
}

// IsPgForeignKeyViolation reports whether err carries SQLSTATE 23503.
func IsPgForeignKeyViolation(err error) bool {
	return hasPgSQLState(err, pgerrcode.ForeignKeyViolation)
}

// IsPgNotNullViolation reports whether err carries SQLSTATE 23502.
func IsPgNotNullViolation(err error) bool {
	return hasPgSQLState(err, pgerrcode.NotNullViolation)
}

// IsPgCheckViolation reports whether err carries SQLSTATE 23514.
func IsPgCheckViolation(err error) bool {
	return hasPgSQLState(err, pgerrcode.CheckViolation)
}

func hasPgSQLState(err error, want string) bool {
	var sqlState interface{ SQLState() string }
	if errors.As(err, &sqlState) {
		return sqlState.SQLState() == want
	}
	return false
}

// JSONB is a thin wrapper around json.RawMessage that satisfies sql.Scanner
// and driver.Valuer for PostgreSQL JSONB columns. The bytes are stored
// verbatim; encoding is the caller's responsibility.
type JSONB json.RawMessage

// Value implements driver.Valuer.
func (j JSONB) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return []byte(j), nil
}

// Scan implements sql.Scanner. Both string and []byte sources are accepted;
// the bytes are copied so the caller may reuse the input buffer.
func (j *JSONB) Scan(value any) error {
	if value == nil {
		*j = nil
		return nil
	}
	switch v := value.(type) {
	case []byte:
		out := make(JSONB, len(v))
		copy(out, v)
		*j = out
	case string:
		*j = JSONB(v)
	default:
		return fmt.Errorf("JSONB.Scan: cannot scan type %T", value)
	}
	return nil
}

// MarshalJSON returns the raw bytes, or "null" when empty.
func (j JSONB) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte("null"), nil
	}
	return j, nil
}

// UnmarshalJSON copies data into j.
func (j *JSONB) UnmarshalJSON(data []byte) error {
	if j == nil {
		return nil
	}
	out := make(JSONB, len(data))
	copy(out, data)
	*j = out
	return nil
}
