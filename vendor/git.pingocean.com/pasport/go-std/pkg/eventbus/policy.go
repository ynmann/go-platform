package eventbus

// BackpressurePolicy chooses how an async subscription reacts when its
// bounded queue is full at Publish time. The policy is set per subscription
// via WithBackpressure (or per bus via WithDefaultBackpressurePolicy).
type BackpressurePolicy int

const (
	// PolicyBlock blocks the publisher until the queue has space, the bus
	// stops, or the subscription is removed. Use when delivery loss is
	// unacceptable and producers can wait.
	PolicyBlock BackpressurePolicy = iota

	// PolicyDropNewest drops the incoming payload when the queue is full.
	// Use when fresh data matters less than throughput (e.g. metrics).
	PolicyDropNewest

	// PolicyDropOldest evicts the oldest queued payload to make room for
	// the newcomer. Use when recent data matters more than historical
	// (e.g. UI state, presence).
	PolicyDropOldest
)

// String returns a stable lowercase tag for telemetry.
func (p BackpressurePolicy) String() string {
	switch p {
	case PolicyBlock:
		return "block"
	case PolicyDropNewest:
		return "drop_newest"
	case PolicyDropOldest:
		return "drop_oldest"
	default:
		return "unknown"
	}
}

// DropReason is reported via Observer.OnDrop when a payload does not reach
// a handler.
type DropReason int

const (
	// DropAfterStop — Publish (or a blocked async send) was preempted by
	// Stop / StopWithContext.
	DropAfterStop DropReason = iota

	// DropQueueFull — async subscription's bounded queue had no room and
	// the policy required a drop.
	DropQueueFull

	// DropNoSubscribers — published payload had nobody listening on the
	// topic. Almost always a configuration bug.
	DropNoSubscribers

	// DropTypeMismatch — Publish[T] and Subscribe[U] disagree on the
	// payload type for the same topic. Always a programming error.
	DropTypeMismatch

	// DropUnsubscribed — async send raced with Unsubscribe and the
	// subscription was torn down before the worker could pick the job.
	DropUnsubscribed
)

func (r DropReason) String() string {
	switch r {
	case DropAfterStop:
		return "after_stop"
	case DropQueueFull:
		return "queue_full"
	case DropNoSubscribers:
		return "no_subscribers"
	case DropTypeMismatch:
		return "type_mismatch"
	case DropUnsubscribed:
		return "unsubscribed"
	default:
		return "unknown"
	}
}
