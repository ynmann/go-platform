// Package ctxkeys provides type-safe, collision-free keys for
// context.Context values.
//
// The standard idiom for context.WithValue uses an unexported struct or
// int as the key to avoid cross-package collisions. That works but is
// boilerplate-heavy and offers no compile-time guarantee that the value
// stored under a key has the expected type — every getter has to assert
// the type and handle a "stored under the right key but wrong type"
// case that should be impossible by construction.
//
// This package collapses the pattern into a single generic primitive,
// [Key]. A Key carries the value type with it: With and Value are
// typed at the call site, and the in-program type is enforced once,
// in one place.
//
// Identity is pointer-based: every call to [New] allocates a fresh
// sentinel, so two Keys are always distinct even when the value type
// and debug name are identical. Copying a Key is cheap and preserves
// identity.
//
// Typical usage is one package-level Key per concept, exposed through
// thin With/From helpers that wrap the Key methods:
//
//	package authctx
//
//	import (
//	    "context"
//
//	    "git.pingocean.com/pasport/go-std/pkg/ctxkeys"
//	)
//
//	var userKey = ctxkeys.New[*User]("auth.user")
//
//	func WithUser(ctx context.Context, u *User) context.Context {
//	    return userKey.With(ctx, u)
//	}
//
//	func UserFromContext(ctx context.Context) (*User, bool) {
//	    return userKey.Value(ctx)
//	}
package ctxkeys

import "context"

// Key is a typed, collision-free context value key.
//
// A Key is created once at package init via [New] and reused. Its zero
// value is unusable.
//
// Keys are comparable: two Key[T] values are equal iff one was copied
// from the other. Two Keys produced by separate New calls are always
// distinct, even if T and the debug name are identical.
type Key[T any] struct {
	id   *keyId
	name string
}

// keyId is the unique identity used as the underlying context key.
// It must be non-zero-size: pointers to a zero-size type are not
// guaranteed by the Go spec to be distinct, so two empty-struct
// allocations could share an address and let unrelated keys collide.
// A single non-zero-size field is enough — a uintptr counter is a
// concise choice and is never read.
type keyId struct{ _ uintptr }

// New allocates a fresh Key[T] with the given debug name.
//
// The name is used only by [Key.String] and panic messages; identity is
// independent of it. Two calls to New with the same name produce two
// distinct keys. New is safe at package init.
func New[T any](name string) Key[T] {
	return Key[T]{id: &keyId{}, name: name}
}

// With returns a derived context that carries v under k.
func (k Key[T]) With(ctx context.Context, v T) context.Context {
	return context.WithValue(ctx, k.id, v)
}

// Value returns the value stored under k and a boolean reporting
// whether one was set. The ok result is false if no value was set, or
// — only across module boundaries with mismatched generic
// instantiations — the stored value is not of type T.
func (k Key[T]) Value(ctx context.Context) (T, bool) {
	v, ok := ctx.Value(k.id).(T)
	return v, ok
}

// MustValue returns the value stored under k. It panics if no value was
// set. Use this in code paths that run strictly downstream of
// middleware documented to populate the key.
func (k Key[T]) MustValue(ctx context.Context) T {
	v, ok := k.Value(ctx)
	if !ok {
		panic("ctxkeys: missing value for key " + k.name)
	}
	return v
}

// ValueOr returns the value stored under k, or fallback if none is set.
func (k Key[T]) ValueOr(ctx context.Context, fallback T) T {
	if v, ok := k.Value(ctx); ok {
		return v
	}
	return fallback
}

// Has reports whether a value of type T is stored under k.
func (k Key[T]) Has(ctx context.Context) bool {
	_, ok := k.Value(ctx)
	return ok
}

// String returns the debug name passed to New.
func (k Key[T]) String() string { return k.name }
