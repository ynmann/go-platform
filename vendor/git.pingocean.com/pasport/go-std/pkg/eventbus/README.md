# eventbus

In-process publish/subscribe runtime for Example services. Topics are opaque
strings, payloads are arbitrary types delivered through generic `Publish` /
`Subscribe` pairs. Domain vocabulary (topic names, event types, envelope
shapes) lives in `pkg/domain` and is wired in by callers — the bus only
owns delivery, ordering, recovery, observability, and shutdown semantics.

```text
import "git.pingocean.com/pasport/go-std/pkg/eventbus"
```

## Why this exists

Most services need a way to fan out internal events (an HTTP handler creates
a `User` → cache invalidator, audit logger and notification builder all want
to react) without coupling those receivers into a shared interface. A bus
solves that. The hard part is doing it well: contention-free under load,
isolated against slow handlers, observable in production, and predictable
during shutdown.

This package keeps that contract.

## Design

```text
   Publish[T] ─────────────►  shardFor(topic)  ─────►  atomic.Pointer[ []sub ]
                                  │ N shards               │ COW slice, sorted
                                  │ no lock on read        │ once at write time
                                  ▼                        ▼
                          deliver: fan-out                handler chain
                                  │
              ┌───────────────────┼────────────────────┐
              ▼                   ▼                    ▼
      sync handler         async sub queue       async sub queue
      (in publisher        ─► worker             ─► worker pool
       goroutine)                                  (partition routed)
```

- **Sharded subscriber index.** The topic→subscribers map is split into
  `max(16, 2*GOMAXPROCS)` shards (rounded to a power of two) keyed by FNV-1a
  of the topic. Subscribe / Unsubscribe across unrelated topics never
  contend.
- **Copy-on-write subscriber lists.** Each shard holds an `atomic.Pointer`
  per topic; writers rebuild and CAS, readers do a single pointer load.
  Publish is fully lock-free. Sorting by priority happens once at write
  time, not on every delivery.
- **Three delivery modes.**
  1. *Synchronous (default).* `Subscribe` + `Publish` — the publisher's
     goroutine runs every handler in priority order. Cheapest, ordering-
     friendly, blocks the publisher.
  2. *Legacy publish-async.* `NewBus(WithBufferSize(N))` — every Publish
     spawns a goroutine for the delivery loop. Fire-and-forget, no
     backpressure, no per-subscriber isolation. Kept for backward
     compatibility.
  3. *Per-subscription async.* `SubscribeAsync(...)` — each subscription
     owns a bounded queue (or N queues with partition routing) and
     dedicated workers. Slow handlers cannot stall peers; backpressure is
     explicit (block / drop-newest / drop-oldest).
- **Partitioned parallelism.** Async subscriptions can opt into N parallel
  workers via `WithPartitionedWorkers(N)`. Payloads implementing
  `Partitioned` are routed by `FNV(key) % N`, preserving per-key ordering
  while allowing unrelated keys to run in parallel — Kafka-partition style.
- **Sequence cursor.** A payload may implement `Sequenceable` to receive a
  monotonic per-bus delivery sequence at Publish time. Every subscriber
  observes the same number for the same publish, useful for ordering
  cross-topic effects.
- **Observability hooks.** Wire an `Observer` via `WithObserver` to receive
  publish, delivery start/end (with latency), drop, panic and queue-depth
  events. Adapt onto Prometheus, OpenTelemetry or a structured logger.

## Quick start

```go
bus := eventbus.NewBus()
defer bus.Stop()

type UserCreated struct{ ID string }

eventbus.Subscribe(bus, eventbus.Topic("user.created"),
    func(ctx context.Context, ev UserCreated) {
        log.Printf("welcome %s", ev.ID)
    },
    eventbus.PriorityNormal,
)

eventbus.Publish(bus, eventbus.Topic("user.created"), UserCreated{ID: "u-1"})
```

## Async subscriptions

Use `SubscribeAsync` when a handler is slow, may block on I/O, or must not
stall peers. The publisher enqueues into the subscription's bounded queue
and returns; a dedicated worker dequeues and invokes the handler.

```go
eventbus.SubscribeAsync(bus, "user.created",
    func(ctx context.Context, ev UserCreated) {
        sendWelcomeEmail(ctx, ev) // takes ~200ms
    },
    eventbus.PriorityNormal,
    eventbus.WithQueueSize(1024),
    eventbus.WithBackpressure(eventbus.PolicyBlock),
)
```

### Backpressure policies

| Policy | Behaviour when queue is full | When to use |
| --- | --- | --- |
| `PolicyBlock` (default) | Publisher blocks until space frees up, the bus stops, or the subscription is removed. | Loss is unacceptable; producers can wait. |
| `PolicyDropNewest` | Incoming payload is dropped, `Observer.OnDrop(DropQueueFull)` fires. | Fresh data matters less than throughput (metrics, telemetry). |
| `PolicyDropOldest` | Oldest queued payload is evicted to make room. | Recency matters more than history (UI state, presence). |

### Partitioned workers

```go
type EntityChanged struct {
    EntityID string
    Body     []byte
}

func (e EntityChanged) PartitionKey() string { return e.EntityID }

eventbus.SubscribeAsync(bus, "entity.changed",
    handle, eventbus.PriorityNormal,
    eventbus.WithPartitionedWorkers(8), // 8 parallel workers
    eventbus.WithQueueSize(512),        // each worker has its own queue
)
```

Within an `EntityID`, events stay strictly ordered (always routed to the
same worker). Between different `EntityID`s, events run in parallel.
Non-`Partitioned` payloads all go to worker 0, so order is preserved.

## Observability

```go
type prom struct {
    eventbus.NoopObserver
    publishes prometheus.Counter
    latency   prometheus.Histogram
    queueDepth *prometheus.GaugeVec
}

func (p *prom) OnPublish(topic string, _ any) {
    p.publishes.Inc()
}
func (p *prom) OnDeliveryEnd(topic string, _ eventbus.SubscriptionInfo, d time.Duration) {
    p.latency.Observe(d.Seconds())
}
func (p *prom) OnQueueDepth(topic string, info eventbus.SubscriptionInfo, depth, capacity int) {
    p.queueDepth.WithLabelValues(topic, info.TypeName).Set(float64(depth))
}

bus := eventbus.NewBus(eventbus.WithObserver(&prom{...}))
```

Observer methods are called on the publish path and inside delivery
workers, so implementations must be non-blocking and safe for concurrent
use. Embed `NoopObserver` to satisfy the interface while overriding only
the hooks you care about.

| Hook | Fires when |
| --- | --- |
| `OnPublish` | Bus has accepted a payload but not yet dispatched it. |
| `OnDeliveryStart` / `OnDeliveryEnd` | Around each handler invocation; `latency` is handler-only. |
| `OnDrop` | Payload did not reach a handler — see `DropReason`. |
| `OnPanic` | Handler panicked; bus-level recovery has already executed. |
| `OnQueueDepth` | After a successful enqueue into an async queue. |

## Middleware

```go
bus := eventbus.NewBus(eventbus.WithMiddleware(
    eventbus.LoggingMiddleware(),
    eventbus.ContextValidationMiddleware(),
))
```

Middleware wraps every handler in registration order; the last registered
is the outermost wrapper at delivery time. Panic recovery is always
attached as the innermost layer — callers do not need to install it.

## Type safety

`Publish[T]` and `Subscribe[U]` must agree on `T == U` for the same topic.
A mismatch is logged and reported via `Observer.OnDrop(DropTypeMismatch)`
at delivery time. The simplest convention is to declare a `Topic` constant
and a payload type next to it:

```go
package domain

type UserCreated struct{ ID string }

const TopicUserCreated eventbus.Topic = "user.created"
```

…and use `domain.TopicUserCreated` from both ends.

## Shutdown

`Bus.Stop()` is idempotent. It signals shutdown, drains in-flight
deliveries (sync handlers complete, async workers drain their queues),
and returns. `StopWithContext(ctx)` adds a deadline — pending deliveries
that exceed `ctx` are abandoned and the call returns `ctx.Err()`.

`Unsubscribe` on an async subscription closes its workers immediately;
items queued at the time of unsubscribe are dropped with
`DropUnsubscribed`.

## Performance

Reference numbers on an Apple M3 Max (run with `go test -bench=. -benchmem`):

| Benchmark | ns/op | B/op | allocs/op |
| --- | --: | --: | --: |
| `BenchmarkSyncPublish` | ~106 | 96 | 2 |
| `BenchmarkAsyncPublish` (legacy `WithBufferSize`) | ~324 | 184 | 4 |
| `BenchmarkWithMiddleware` | ~124 | 112 | 3 |

Use `SubscribeAsync` with appropriate queue sizing for production paths
that fan out to slow consumers; the legacy `WithBufferSize` mode is
unbounded and exists for backward compatibility.

## Concurrency invariants

- Publish is lock-free on the hot path: one `sync.Map` Load + one
  `atomic.Pointer` Load per topic.
- Subscribe / Unsubscribe rebuild the per-topic slice under a CAS retry
  loop; readers never see a partially mutated list.
- Handlers run on:
  - the publisher's goroutine (sync mode),
  - a fresh goroutine per Publish (legacy `WithBufferSize`),
  - or one of the subscription's worker goroutines (async mode).
- Recovery wraps every handler. A panicking handler does not abort
  delivery to peers on the same topic.

## When *not* to use this

- For cross-process / cross-host delivery, use `pkg/mq/nats/*` (with the
  outbox/inbox pair if transactional semantics are required).
- For a durable event log with replay or late subscribers, this is the
  wrong primitive — `eventbus` is intentionally in-process and ephemeral.
