package eventbus

import "time"

// SubscriptionInfo describes a subscription for Observer hook purposes.
// Fields are immutable for the lifetime of the subscription.
type SubscriptionInfo struct {
	TypeName string
	Priority Priority
	Async    bool
}

// Observer receives lifecycle events from the bus. Hooks fire on the
// publish path and inside delivery workers, so implementations must be
// non-blocking and safe for concurrent use.
//
// The default bus uses NoopObserver. Wire a real implementation via
// WithObserver — typically a thin adapter over Prometheus, OpenTelemetry,
// or a structured logger.
type Observer interface {
	// OnPublish fires after the bus has accepted the payload but before
	// it has been dispatched to any subscriber.
	OnPublish(topic string, payload any)

	// OnDeliveryStart fires immediately before a handler is invoked.
	OnDeliveryStart(topic string, sub SubscriptionInfo)

	// OnDeliveryEnd fires after a handler returns (including after a
	// recovered panic). latency measures handler-only execution.
	OnDeliveryEnd(topic string, sub SubscriptionInfo, latency time.Duration)

	// OnDrop fires when a payload does not reach a handler. See
	// DropReason for the enumerated cases.
	OnDrop(topic string, payload any, reason DropReason)

	// OnPanic fires when a handler panicked. Bus-level recovery has
	// already executed by the time this hook is called; delivery to peer
	// subscribers continues unaffected.
	OnPanic(topic string, sub SubscriptionInfo, recovered any)

	// OnQueueDepth fires after a successful enqueue into an async
	// subscription's queue. depth is the post-send length, capacity the
	// channel cap. Use this to feed a histogram and detect slow consumers.
	OnQueueDepth(topic string, sub SubscriptionInfo, depth, capacity int)
}

// NoopObserver implements Observer with no-op methods. Use it as the zero
// value or embed it to satisfy the interface while overriding only the
// hooks you care about.
type NoopObserver struct{}

func (NoopObserver) OnPublish(string, any)                                 {}
func (NoopObserver) OnDeliveryStart(string, SubscriptionInfo)              {}
func (NoopObserver) OnDeliveryEnd(string, SubscriptionInfo, time.Duration) {}
func (NoopObserver) OnDrop(string, any, DropReason)                        {}
func (NoopObserver) OnPanic(string, SubscriptionInfo, any)                 {}
func (NoopObserver) OnQueueDepth(string, SubscriptionInfo, int, int)       {}
