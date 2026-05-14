package eventbus

import (
	"context"

	"github.com/rs/zerolog/log"
)

// MiddlewareFunc wraps a handler to add cross-cutting behaviour. It is
// applied at delivery time in registration order, with the last registered
// middleware as the outermost wrapper (LIFO during execution).
//
// Panic recovery is always added as the innermost layer by buildChain;
// callers do not need to install it themselves.
type MiddlewareFunc func(next func(context.Context, any)) func(context.Context, any)

// LoggingMiddleware logs the payload type before each delivery. Useful in
// development; disable in production to keep log volume sane.
func LoggingMiddleware() MiddlewareFunc {
	return func(next func(context.Context, any)) func(context.Context, any) {
		return func(ctx context.Context, payload any) {
			log.Debug().Msgf("[eventbus] delivering %T", payload)

			next(ctx, payload)
		}
	}
}

// ContextValidationMiddleware drops a delivery if ctx is already cancelled
// or expired. Add it when handlers do I/O that should never start past a
// deadline.
func ContextValidationMiddleware() MiddlewareFunc {
	return func(next func(context.Context, any)) func(context.Context, any) {
		return func(ctx context.Context, payload any) {
			select {
			case <-ctx.Done():
				return

			default:
				next(ctx, payload)
			}
		}
	}
}

// RecoveryMiddleware is a passthrough kept for API parity — recovery is
// always attached as the innermost layer, so this middleware is intentionally
// a no-op.
func RecoveryMiddleware() MiddlewareFunc {
	return func(next func(context.Context, any)) func(context.Context, any) {
		return next
	}
}
