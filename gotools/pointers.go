package gotools

import (
	"errors"
	"reflect"
)

// Ptr returns a pointer to v.
func Ptr[T any](v T) *T {
	return &v
}

// Deref returns the value p points to, or T's zero value if p is nil.
func Deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// OptionalToPtr returns nil for T's zero value, otherwise &v.
func OptionalToPtr[T comparable](v T) *T {
	var z T
	if v == z {
		return nil
	}
	return &v
}

// OptionalFromPtr returns *v, or T's zero value if v is nil.
func OptionalFromPtr[T comparable](v *T) T {
	var z T
	if v == nil {
		return z
	}
	return *v
}

// OptionalString converts empty string to nil.
func OptionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// PtrIfNotEmpty is an alias for OptionalString kept for call-site
// compatibility across repos.
func PtrIfNotEmpty(v string) *string {
	return OptionalString(v)
}

// PutIfNotNil returns &value if cond != nil, else nil.
func PutIfNotNil[T any](cond any, value T) *T {
	if cond == nil {
		return nil
	}
	return &value
}

// IsPointer reports whether value is a pointer.
func IsPointer(value any) bool {
	v := reflect.ValueOf(value)
	return v.Kind() == reflect.Ptr
}

// Cast performs a type assertion to T, returning the zero value + error on mismatch.
func Cast[T any](val any, to T) (T, error) {
	var zero T

	ret, ok := val.(T)
	if !ok {
		return zero, errors.New("failed to cast")
	}

	return ret, nil
}
