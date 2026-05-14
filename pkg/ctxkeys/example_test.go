package ctxkeys_test

import (
	"context"
	"fmt"

	"git.pingocean.com/pasport/go-std/pkg/ctxkeys"
)

// ExampleKey demonstrates the typical pattern: declare one Key per
// concept and wrap it in domain-specific helpers.
func ExampleKey() {
	type RequestId string

	requestIdKey := ctxkeys.New[RequestId]("request.id")

	withRequestId := func(ctx context.Context, id RequestId) context.Context {
		return requestIdKey.With(ctx, id)
	}
	requestIdFromContext := func(ctx context.Context) (RequestId, bool) {
		return requestIdKey.Value(ctx)
	}

	ctx := withRequestId(context.Background(), "req-42")

	if id, ok := requestIdFromContext(ctx); ok {
		fmt.Println(id)
	}
	// Output: req-42
}
