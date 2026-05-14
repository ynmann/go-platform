package eventbus

import (
	"context"
	"reflect"

	"github.com/rs/zerolog/log"
)

// SubscriptionHandle is the only way to remove a subscription. Identity is
// pointer-based: each Subscribe / SubscribeAsync call yields exactly one
// handle backed by one internal subscription record.
type SubscriptionHandle interface {
	Unsubscribe()
}

// Publish delivers payload to every subscriber registered for topic.
//
// The K type parameter accepts any string-derived type — pass plain strings
// or typed Topic values from a domain package without conversion.
//
// T must be the exact type used in the matching Subscribe call. Mismatches
// are logged at delivery time (see warnTypeMismatch) and reported via
// Observer.OnDrop with DropTypeMismatch — this is always a programming error.
func Publish[K ~string, T any](b Bus, topic K, payload T) {
	PublishWithContext(context.Background(), b, topic, payload)
}

// PublishWithContext is Publish with an explicit context propagated to
// every handler. Prefer this form when ctx carries a deadline, trace span
// or cancellation signal.
func PublishWithContext[K ~string, T any](ctx context.Context, b Bus, topic K, payload T) {
	impl, ok := b.(*bus)
	if !ok {
		log.Error().
			Str("topic", string(topic)).
			Msg("[eventbus] Publish called with a non-*bus implementation — this is a programming error")

		return
	}

	impl.publish(ctx, string(topic), payload)
}

// Subscribe registers handler to receive payloads of type T on topic.
// Delivery is synchronous: handler runs inside the publisher's goroutine.
//
// Contract:
//   - T must match the type used by the Publish call site for this topic.
//   - Each Subscribe registration is independent; two calls with the same
//     handler value receive the event twice.
//   - Handlers run in priority order; ties keep registration order.
//   - Every handler is wrapped with panic recovery automatically.
//   - The returned SubscriptionHandle is the sole removal mechanism.
func Subscribe[K ~string, T any](
	b Bus,
	topic K,
	handler func(context.Context, T),
	priority Priority,
) SubscriptionHandle {
	return subscribeImpl(b, topic, handler, priority, nil)
}

// SubscribeAsync registers handler to receive payloads of type T on topic
// via a per-subscription bounded queue. The publisher does not run handler
// — instead, it enqueues the payload and the subscription's worker pool
// dequeues and invokes handler.
//
// This form gives slow handlers their own queue (so they cannot stall
// peers on the same topic), explicit backpressure (PolicyBlock /
// PolicyDropNewest / PolicyDropOldest), and optional partitioned
// parallelism (WithPartitionedWorkers). Default queue size and policy
// come from the Bus options (WithDefaultAsyncQueueSize,
// WithDefaultBackpressurePolicy).
//
// Ordering: with workers == 1 (the default), per-subscription order is
// preserved and matches publish order. With workers > 1, payloads
// implementing Partitioned are routed by FNV(key) % workers so each
// partition is single-threaded; non-Partitioned payloads all go to
// worker 0.
func SubscribeAsync[K ~string, T any](
	b Bus,
	topic K,
	handler func(context.Context, T),
	priority Priority,
	opts ...AsyncOption,
) SubscriptionHandle {
	cfg := asyncConfig{workers: 1}
	for _, o := range opts {
		o(&cfg)
	}

	return subscribeImpl(b, topic, handler, priority, &cfg)
}

// AsyncOption configures a single async subscription.
type AsyncOption func(*asyncConfig)

type asyncConfig struct {
	queueSize int
	workers   int
	policy    BackpressurePolicy
	queueSet  bool
	policySet bool
}

// WithQueueSize sets the per-worker queue capacity. Overrides
// WithDefaultAsyncQueueSize for this subscription.
func WithQueueSize(n int) AsyncOption {
	return func(c *asyncConfig) {
		if n > 0 {
			c.queueSize = n
			c.queueSet = true
		}
	}
}

// WithBackpressure sets the policy used when the queue is full. Overrides
// WithDefaultBackpressurePolicy for this subscription.
func WithBackpressure(p BackpressurePolicy) AsyncOption {
	return func(c *asyncConfig) {
		c.policy = p
		c.policySet = true
	}
}

// WithPartitionedWorkers enables N parallel workers for the subscription
// and routes payloads by Partitioned key. When the payload does not
// implement Partitioned, all events go to worker 0 (preserving order).
//
// Each worker has its own queue of the configured size, so total queue
// capacity is N * queueSize.
func WithPartitionedWorkers(n int) AsyncOption {
	return func(c *asyncConfig) {
		if n > 0 {
			c.workers = n
		}
	}
}

func subscribeImpl[K ~string, T any](
	b Bus,
	topic K,
	handler func(context.Context, T),
	priority Priority,
	ac *asyncConfig,
) SubscriptionHandle {
	impl, ok := b.(*bus)
	if !ok {
		log.Error().
			Str("topic", string(topic)).
			Msg("[eventbus] Subscribe called with a non-*bus implementation — this is a programming error")

		return noopHandle{}
	}

	typeName := reflect.TypeOf((*T)(nil)).Elem().String()
	topicStr := string(topic)

	wrapper := func(ctx context.Context, raw any) {
		typed, ok := raw.(T)
		if !ok {
			warnTypeMismatch(topicStr, typeName, reflect.TypeOf(raw).String())
			impl.observer.OnDrop(topicStr, raw, DropTypeMismatch)

			return
		}

		handler(ctx, typed)
	}

	sub := &subscription{
		handler:  wrapper,
		priority: priority,
		typeName: typeName,
	}

	if ac != nil {
		sub.async = newAsyncSub(impl, *ac)
		sub.async.start(impl, sub, topicStr)
	}

	impl.shardFor(topicStr).addSub(topicStr, sub)

	return &subHandle{bus: impl, topic: topicStr, sub: sub}
}

type subHandle struct {
	bus   *bus
	topic string
	sub   *subscription
}

func (h *subHandle) Unsubscribe() {
	h.bus.shardFor(h.topic).removeSub(h.topic, h.sub)

	if h.sub.async != nil {
		h.sub.async.close()
	}
}

// noopHandle is the safe fallback returned when Subscribe is given a Bus
// implementation it does not own — Unsubscribe becomes a no-op, mirroring
// "the subscription was never registered".
type noopHandle struct{}

func (noopHandle) Unsubscribe() {}
