// Package eventbus is an in-process publish/subscribe runtime.
//
// The bus is content-agnostic: topics are opaque strings and payloads are
// arbitrary values delivered by generic Publish / Subscribe pairs. Domain
// vocabulary (topic names, event types, envelope shapes) lives in
// pkg/domain and is wired in by callers — the bus only owns delivery,
// ordering, recovery, observability and shutdown semantics.
//
// # Architecture
//
// The subscriber index is sharded into N maps (default max(16, 2*GOMAXPROCS),
// power-of-two) keyed by FNV-1a hash of the topic. Inside each shard, the
// per-topic subscriber list is kept behind atomic.Pointer with copy-on-write
// updates: Subscribe / Unsubscribe rebuild the slice and CAS it in, Publish
// loads the pointer without taking any lock. This makes the hot Publish path
// fully lock-free and removes per-publish sorting — the slice is sorted
// once at write time.
//
// # Delivery modes
//
//  1. Synchronous (default): Subscribe + Publish — the publisher's goroutine
//     runs every handler in priority order. Fastest, ordering-friendly,
//     blocks the publisher.
//
//  2. Legacy publish-async: NewBus(WithBufferSize(N)) — every Publish spawns
//     a goroutine for the delivery loop. Fire-and-forget, no backpressure,
//     no per-subscriber isolation. Kept for backward compatibility.
//
//  3. Per-subscription async: SubscribeAsync(...) — each subscription owns a
//     bounded queue (or N queues with partition routing) and dedicated
//     workers. Slow handlers cannot stall peers; backpressure policy is
//     explicit (block / drop-newest / drop-oldest).
//
// # Ordering
//
// Within a single sync subscriber chain, handlers run in priority order,
// ties keep registration order. Across topics there is no global ordering.
// A payload may opt into a monotonic per-bus delivery cursor by implementing
// Sequenceable — the bus stamps the next sequence value before fan-out, so
// every subscriber observes the same number for the same publish.
//
// For async subscriptions with WithPartitionedWorkers, payloads implementing
// Partitioned are routed by FNV(key) % workers, preserving per-key ordering
// while parallelising unrelated keys (Kafka-partition style).
package eventbus

import (
	"context"
	"hash/fnv"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// Topic is a convenience alias for callers that want a plain string topic.
// Domain packages typically declare their own string-derived Topic type;
// Publish and Subscribe accept any such type via the ~string constraint, so
// no conversion is needed at the call site.
type Topic = string

// Sequenceable is implemented by payloads that want the bus to stamp them
// with a monotonic per-bus delivery sequence at publish time. Implementations
// should be idempotent so manual republishes preserve the original cursor.
type Sequenceable interface {
	AssignSeq(int64)
}

// Bus is the only handle consumers retain. Publish, Subscribe and
// SubscribeAsync are package-level generics that take a Bus — the
// interface stays minimal while generics carry full payload type-safety.
type Bus interface {
	// Stop drains in-flight deliveries and closes the bus. Idempotent.
	// After Stop returns, further Publish calls are dropped with a warning
	// log and DropAfterStop is reported to the Observer.
	Stop() error

	// StopWithContext is Stop with a deadline: it returns ctx.Err() if ctx
	// fires before deliveries complete.
	StopWithContext(ctx context.Context) error
}

// NewBus constructs a Bus. The zero-option form is synchronous with no
// middleware, default sharding, and a NoopObserver — appropriate for tests
// and ordering-sensitive paths.
func NewBus(opts ...Option) Bus {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}

	shards := make([]*shard, cfg.shards)
	for i := range shards {
		shards[i] = newShard()
	}

	return &bus{
		shards:         shards,
		shardMask:      uint64(cfg.shards - 1),
		middleware:     cfg.middleware,
		observer:       cfg.observer,
		bufferSize:     cfg.bufferSize,
		asyncQueueSize: cfg.asyncQueueSize,
		asyncPolicy:    cfg.asyncPolicy,
		shutdown:       make(chan struct{}),
	}
}

type bus struct {
	shards         []*shard
	shardMask      uint64
	middleware     []MiddlewareFunc
	observer       Observer
	bufferSize     int
	asyncQueueSize int
	asyncPolicy    BackpressurePolicy

	shutdown   chan struct{}
	shutdownOK atomic.Bool
	pending    sync.WaitGroup
	seq        atomic.Int64
}

type subscription struct {
	handler  func(context.Context, any)
	priority Priority
	typeName string

	// async is non-nil iff the subscription was created via SubscribeAsync.
	// When set, deliver dispatches into the bounded queue instead of
	// invoking handler inline.
	async *asyncSub
}

func (b *bus) shardFor(topic string) *shard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(topic))

	return b.shards[h.Sum64()&b.shardMask]
}

func (b *bus) Stop() error {
	return b.StopWithContext(context.Background())
}

func (b *bus) StopWithContext(ctx context.Context) error {
	b.signalStop()

	done := make(chan struct{})
	go func() {
		b.pending.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *bus) signalStop() {
	if b.shutdownOK.CompareAndSwap(false, true) {
		close(b.shutdown)
	}
}

func (b *bus) publish(ctx context.Context, topic string, payload any) {
	select {
	case <-b.shutdown:
		b.observer.OnDrop(topic, payload, DropAfterStop)
		log.Warn().
			Str("topic", topic).
			Msgf("[eventbus] publish after Stop — %T dropped", payload)

		return

	default:
	}

	if seq, ok := payload.(Sequenceable); ok {
		seq.AssignSeq(b.seq.Add(1))
	}

	b.observer.OnPublish(topic, payload)

	if b.bufferSize > 0 {
		b.pending.Go(func() {
			b.deliver(ctx, topic, payload)
		})

		return
	}

	b.deliver(ctx, topic, payload)
}

func (b *bus) deliver(ctx context.Context, topic string, payload any) {
	snapshot := b.shardFor(topic).load(topic)
	if len(snapshot) == 0 {
		b.observer.OnDrop(topic, payload, DropNoSubscribers)
		log.Warn().
			Str("topic", topic).
			Msgf("[eventbus] no subscribers for %T — did you forget to Subscribe?", payload)

		return
	}

	for _, s := range snapshot {
		if s.async != nil {
			b.dispatchAsync(ctx, topic, s, payload)

			continue
		}

		b.invokeChain(ctx, topic, s, payload)
	}
}

// invokeChain runs handler with middleware and observer hooks. Used both
// on the sync path (publisher's goroutine) and inside async workers.
func (b *bus) invokeChain(ctx context.Context, topic string, s *subscription, payload any) {
	info := SubscriptionInfo{
		TypeName: s.typeName,
		Priority: s.priority,
		Async:    s.async != nil,
	}

	b.observer.OnDeliveryStart(topic, info)
	start := time.Now()

	chain := buildChain(s.handler, b.middleware, b.observer, topic, info)
	chain(ctx, payload)

	b.observer.OnDeliveryEnd(topic, info, time.Since(start))
}

// buildChain wraps handler with the registered middleware and an innermost
// recovery layer. Recovery is non-negotiable: handler panics must never
// abort delivery to peer subscribers on the same topic.
func buildChain(
	handler func(context.Context, any),
	chain []MiddlewareFunc,
	obs Observer,
	topic string,
	info SubscriptionInfo,
) func(context.Context, any) {
	h := withRecovery(handler, obs, topic, info)
	for i := len(chain) - 1; i >= 0; i-- {
		h = chain[i](h)
	}

	return h
}

func withRecovery(
	next func(context.Context, any),
	obs Observer,
	topic string,
	info SubscriptionInfo,
) func(context.Context, any) {
	return func(ctx context.Context, payload any) {
		defer func() {
			if r := recover(); r != nil {
				obs.OnPanic(topic, info, r)
				log.Error().Msgf("[eventbus] handler panic recovered: %v\n%s", r, debug.Stack())
			}
		}()

		next(ctx, payload)
	}
}

// warnTypeMismatch logs the always-a-bug case where Publish[T] and
// Subscribe[U] disagree on the payload type for the same topic.
func warnTypeMismatch(topic, want, got string) {
	log.Error().
		Str("topic", topic).
		Str("subscribed_type", want).
		Str("published_type", got).
		Msg("[eventbus] type mismatch — Publish and Subscribe use different types for this topic; handler will never fire")
}
