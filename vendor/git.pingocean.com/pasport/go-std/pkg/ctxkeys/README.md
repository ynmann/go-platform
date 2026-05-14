# ctxkeys — type-safe context value keys

`pkg/ctxkeys` is a one-type library for declaring `context.Context`
value keys that are **collision-free** across packages and
**type-checked** at the call site. It replaces the standard idiom

```go
type userKey struct{}

func WithUser(ctx context.Context, u *User) context.Context {
    return context.WithValue(ctx, userKey{}, u)
}

func UserFromContext(ctx context.Context) (*User, bool) {
    u, ok := ctx.Value(userKey{}).(*User)
    return u, ok
}
```

with one line of plumbing per concept:

```go
var userKey = ctxkeys.New[*User]("auth.user")
```

The `With` / `Value` methods on `userKey` are already typed to `*User`.
You can keep the `WithUser` / `UserFromContext` wrappers for readability
at call sites — they become trivial pass-throughs.

## Why bother

The standard idiom has three failure modes that compound at scale:

1. **Boilerplate per key.** A new struct type and two helpers for every
   value carried through `Context`. Repetitive code rots: assertions
   drift from definitions, copy-paste keys reuse the wrong type.
2. **No type guarantee at the call site.** `ctx.Value(k)` returns
   `any`. Every getter must repeat the same type assertion, and a
   wrong-type write earlier in the request flow surfaces as a silent
   `false` from the getter rather than a compile error.
3. **Easy to collide accidentally.** Using a string or `int` constant
   as a key — still common — collides across packages. Unexported
   struct types fix that, but require yet another type per key.

A `ctxkeys.Key[T]` carries the value type and a unique identity in one
small value, so the only thing you write per concept is the variable.

## Quick start

```go
package authctx

import (
    "context"

    "git.pingocean.com/pasport/go-std/pkg/ctxkeys"
)

type User struct {
    Id   string
    Name string
}

var userKey = ctxkeys.New[*User]("auth.user")

func WithUser(ctx context.Context, u *User) context.Context {
    return userKey.With(ctx, u)
}

func UserFromContext(ctx context.Context) (*User, bool) {
    return userKey.Value(ctx)
}
```

For values that middleware is required to populate, the panicking
variant makes the contract explicit at the call site:

```go
func handle(ctx context.Context) {
    u := userKey.MustValue(ctx) // panics if middleware missed it
    // ...
}
```

## API

### `func New[T any](name string) Key[T]`

Allocates a fresh `Key[T]`. The name is used only by `String()` and
panic messages; it does **not** affect identity. Two `New` calls with
the same name produce two distinct keys. Safe at package init.

### `Key[T]`

A typed, collision-free context key. `Key` values are comparable; two
copies of the same `New` result are equal, two separate `New` results
are not. The zero value is unusable.

| Method | Behavior |
| ------ | -------- |
| `With(ctx, v) context.Context`        | Returns a derived context that carries `v` under the key. |
| `Value(ctx) (T, bool)`                | Returns the stored value, or zero+`false` if none was set. |
| `MustValue(ctx) T`                    | Returns the stored value, panics if missing. |
| `ValueOr(ctx, fallback) T`            | Returns the stored value, or `fallback` if missing. |
| `Has(ctx) bool`                       | Reports whether a value is set. |
| `String() string`                     | Returns the debug name passed to `New`. |

## Behavior details

- **Identity is pointer-based.** `New` allocates a fresh sentinel for
  every call. The sentinel type has a non-zero size on purpose: Go does
  not guarantee distinct pointers for zero-size allocations, so two
  empty-struct sentinels could share an address and let unrelated keys
  collide.
- **Copying a `Key` is cheap.** A `Key[T]` is a small value
  (`{*sentinel, string}`). Copies share the same sentinel pointer, so
  every copy refers to the same context value.
- **Type safety is enforced once.** `Value` performs a single type
  assertion against `T`. Within a single program this assertion always
  succeeds because `With` is the only writer. Across module boundaries
  (e.g. a plugin compiled against a different module version) the
  assertion can fail and `Value` returns `(zero, false)` — same shape
  as a missing value.
- **No global registry.** Keys are normal Go variables. Lifetime is
  whatever you give them (usually the lifetime of the program).
- **No reflection, no maps, no locks.** All operations bottom out in
  `context.WithValue` / `context.Value`.

## Patterns

### One key per concept

Declare keys at package scope, expose typed `WithX` / `XFromContext`
wrappers. Wrappers are not strictly required, but they keep call sites
domain-flavored and let you change the storage strategy later without
churning every consumer.

```go
var (
    requestIdKey = ctxkeys.New[string]("trace.request_id")
    tenantKey    = ctxkeys.New[*Tenant]("auth.tenant")
)
```

### gRPC / HTTP middleware

Use `With` in middleware, `Value` (or `MustValue`) in handlers. The
key variable lives in a package both sides import — usually a small
`ctxkeys` or `httpctx` package next to your transport layer.

```go
// middleware
ctx := tenantKey.With(req.Context(), tenant)
next.ServeHTTP(w, req.WithContext(ctx))

// handler
tenant, ok := tenantKey.Value(r.Context())
if !ok {
    http.Error(w, "missing tenant", http.StatusInternalServerError)
    return
}
```

### Optional values with sensible defaults

`ValueOr` is the sugared form of "fall back if upstream did not
populate this":

```go
attempt := retryKey.ValueOr(ctx, 0)
```

### Required values

When a missing value indicates a programmer error rather than a runtime
condition (e.g. a handler that runs only after auth middleware),
`MustValue` puts the contract in the call site:

```go
user := userKey.MustValue(ctx) // panic message includes the key name
```

## Testing

```sh
go test ./pkg/ctxkeys/...
```

The package has no external dependencies and no I/O, so its tests run
in milliseconds.

## What the package deliberately does not do

- **No string-keyed lookup.** There is no global "give me the key
  named `auth.user`" function. The debug name is for humans; the key
  variable is the only handle. Looking keys up by name would defeat
  the collision-free guarantee.
- **No serialization.** `Key` values are program-local identities.
  They cannot be marshalled, RPCed, or compared across processes.
- **No metrics, no logging.** Reading and writing a context value
  should not perform I/O.
- **No cross-context copy helper.** Forwarding a fixed set of values
  from a request context to a background context is application
  logic — write it once, where the contexts hand off, instead of
  generalising it here.
