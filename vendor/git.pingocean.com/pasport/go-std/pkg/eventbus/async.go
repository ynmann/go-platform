package eventbus

import (
	"context"
	"sync/atomic"
)

// asyncSub holds the per-subscription state for SubscribeAsync: one or
// more bounded queues and a worker per queue. Multiple queues are used
// only when the subscription opts into partitioned workers — payloads
// are then routed by FNV(PartitionKey) % workers, which preserves
// per-key ordering while parallelising unrelated keys.
//
// Lifecycle:
//
//   - start spawns N worker goroutines (one per queue) and tracks them on
//     the bus pending wait group.
//   - dispatchAsync pushes jobs into the chosen queue, honouring the
//     configured BackpressurePolicy.
//   - close (called from Unsubscribe) signals workers to exit by closing
//     stop. Pending jobs in the queue are dropped with DropUnsubscribed.
//   - bus.Stop closes b.shutdown; workers drain remaining jobs and exit.
//
// Channels are never closed by the producer — closing while a sender
// might still be in flight would panic. Workers exit on the stop signal
// and rely on the sender-side select on b.shutdown to unblock.
type asyncSub struct {
	queues  []chan asyncJob
	workers int
	policy  BackpressurePolicy
	stop    chan struct{}
	closed  atomic.Bool
}

type asyncJob struct {
	ctx     context.Context
	payload any
}

func newAsyncSub(b *bus, ac asyncConfig) *asyncSub {
	queueSize := ac.queueSize
	if !ac.queueSet {
		queueSize = b.asyncQueueSize
	}

	policy := ac.policy
	if !ac.policySet {
		policy = b.asyncPolicy
	}

	workers := ac.workers
	if workers < 1 {
		workers = 1
	}

	queues := make([]chan asyncJob, workers)
	for i := range queues {
		queues[i] = make(chan asyncJob, queueSize)
	}

	return &asyncSub{
		queues:  queues,
		workers: workers,
		policy:  policy,
		stop:    make(chan struct{}),
	}
}

func (a *asyncSub) start(b *bus, sub *subscription, topic string) {
	for i := range a.queues {
		idx := i
		b.pending.Go(func() {
			a.workerLoop(b, sub, topic, idx)
		})
	}
}

func (a *asyncSub) workerLoop(b *bus, sub *subscription, topic string, idx int) {
	q := a.queues[idx]

	for {
		select {
		case <-a.stop:
			a.drainAndDrop(b, topic, q, DropUnsubscribed)

			return

		case <-b.shutdown:
			a.drainAndDeliver(b, sub, topic, q)

			return

		case job := <-q:
			b.invokeChain(job.ctx, topic, sub, job.payload)
		}
	}
}

func (a *asyncSub) drainAndDeliver(b *bus, sub *subscription, topic string, q chan asyncJob) {
	for {
		select {
		case job := <-q:
			b.invokeChain(job.ctx, topic, sub, job.payload)

		default:
			return
		}
	}
}

func (a *asyncSub) drainAndDrop(b *bus, topic string, q chan asyncJob, reason DropReason) {
	for {
		select {
		case job := <-q:
			b.observer.OnDrop(topic, job.payload, reason)

		default:
			return
		}
	}
}

// close signals the workers to exit without delivering remaining jobs.
// Idempotent.
func (a *asyncSub) close() {
	if a.closed.CompareAndSwap(false, true) {
		close(a.stop)
	}
}

// dispatchAsync routes a payload into the appropriate queue and applies
// the configured BackpressurePolicy when the queue is full. The publish
// path may block (PolicyBlock), drop the new payload (PolicyDropNewest),
// or evict the oldest queued payload (PolicyDropOldest).
func (b *bus) dispatchAsync(ctx context.Context, topic string, sub *subscription, payload any) {
	a := sub.async

	if a.closed.Load() {
		b.observer.OnDrop(topic, payload, DropUnsubscribed)

		return
	}

	idx := 0
	if a.workers > 1 {
		if p, ok := payload.(Partitioned); ok {
			idx = partitionIndex(p.PartitionKey(), a.workers)
		}
	}

	q := a.queues[idx]
	job := asyncJob{ctx: ctx, payload: payload}

	info := SubscriptionInfo{
		TypeName: sub.typeName,
		Priority: sub.priority,
		Async:    true,
	}

	switch a.policy {
	case PolicyBlock:
		select {
		case q <- job:
			b.observer.OnQueueDepth(topic, info, len(q), cap(q))

		case <-b.shutdown:
			b.observer.OnDrop(topic, payload, DropAfterStop)

		case <-a.stop:
			b.observer.OnDrop(topic, payload, DropUnsubscribed)
		}

	case PolicyDropNewest:
		select {
		case q <- job:
			b.observer.OnQueueDepth(topic, info, len(q), cap(q))

		default:
			b.observer.OnDrop(topic, payload, DropQueueFull)
		}

	case PolicyDropOldest:
		for {
			select {
			case q <- job:
				b.observer.OnQueueDepth(topic, info, len(q), cap(q))

				return

			case <-b.shutdown:
				b.observer.OnDrop(topic, payload, DropAfterStop)

				return

			case <-a.stop:
				b.observer.OnDrop(topic, payload, DropUnsubscribed)

				return

			default:
				select {
				case dropped := <-q:
					b.observer.OnDrop(topic, dropped.payload, DropQueueFull)

				default:
					// Lost the race — another goroutine drained.
					// Loop and retry the send.
				}
			}
		}
	}
}
