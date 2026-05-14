package ctxkeys_test

import (
	"context"
	"testing"

	"git.pingocean.com/pasport/go-std/pkg/ctxkeys"
)

func TestKey_RoundTrip(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[string]("request.id")
	ctx := k.With(context.Background(), "abc")

	got, ok := k.Value(ctx)
	if !ok {
		t.Fatalf("Value: ok = false, want true")
	}
	if got != "abc" {
		t.Fatalf("Value: got %q, want %q", got, "abc")
	}
}

func TestKey_MissingReportsZero(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[int]("attempt")

	got, ok := k.Value(context.Background())
	if ok {
		t.Fatalf("Value: ok = true on empty context")
	}
	if got != 0 {
		t.Fatalf("Value: got %d, want zero", got)
	}
}

func TestKey_DistinctSameType(t *testing.T) {
	t.Parallel()

	a := ctxkeys.New[string]("a")
	b := ctxkeys.New[string]("a")

	ctx := a.With(context.Background(), "from-a")

	if v, ok := b.Value(ctx); ok {
		t.Fatalf("b.Value: ok = true with v=%q; keys must not collide", v)
	}
}

func TestKey_OverwriteWithChild(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[string]("name")
	ctx := k.With(context.Background(), "outer")
	ctx = k.With(ctx, "inner")

	if got, _ := k.Value(ctx); got != "inner" {
		t.Fatalf("Value: got %q, want %q", got, "inner")
	}
}

func TestKey_HasAndValueOr(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[int]("retries")

	if k.Has(context.Background()) {
		t.Fatalf("Has: true on empty context")
	}
	if got := k.ValueOr(context.Background(), 7); got != 7 {
		t.Fatalf("ValueOr: got %d, want 7", got)
	}

	ctx := k.With(context.Background(), 3)
	if !k.Has(ctx) {
		t.Fatalf("Has: false after With")
	}
	if got := k.ValueOr(ctx, 7); got != 3 {
		t.Fatalf("ValueOr: got %d, want 3", got)
	}
}

func TestKey_MustValue(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[string]("required")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("MustValue: no panic on missing value")
		}
	}()
	_ = k.MustValue(context.Background())
}

func TestKey_MustValuePresent(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[string]("required")
	ctx := k.With(context.Background(), "ok")

	if got := k.MustValue(ctx); got != "ok" {
		t.Fatalf("MustValue: got %q, want %q", got, "ok")
	}
}

func TestKey_PointerValue(t *testing.T) {
	t.Parallel()

	type user struct{ Name string }

	k := ctxkeys.New[*user]("user")
	want := &user{Name: "alice"}

	ctx := k.With(context.Background(), want)
	got, ok := k.Value(ctx)
	if !ok || got != want {
		t.Fatalf("Value: got=%v ok=%v, want=%v ok=true", got, ok, want)
	}
}

func TestKey_CopyPreservesIdentity(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[string]("copy")
	kCopy := k

	ctx := k.With(context.Background(), "x")
	if got, ok := kCopy.Value(ctx); !ok || got != "x" {
		t.Fatalf("kCopy.Value: got=%q ok=%v, want=%q ok=true", got, ok, "x")
	}
}

func TestKey_String(t *testing.T) {
	t.Parallel()

	k := ctxkeys.New[int]("debug.name")
	if k.String() != "debug.name" {
		t.Fatalf("String: got %q, want %q", k.String(), "debug.name")
	}
}
