package eventbus

import "runtime"

// Option configures a Bus at construction time.
type Option func(*config)

type config struct {
	shards         int
	bufferSize     int
	asyncQueueSize int
	asyncPolicy    BackpressurePolicy
	middleware     []MiddlewareFunc
	observer       Observer
}

func defaultConfig() *config {
	return &config{
		shards:         defaultShards(),
		asyncQueueSize: 1024,
		asyncPolicy:    PolicyBlock,
		observer:       NoopObserver{},
	}
}

// defaultShards picks a power-of-two count proportional to the CPU budget.
// The minimum of 16 keeps low-core machines (CI runners, small VMs) from
// degenerating into a single hot mutex while still bounding the number of
// maps and atomic.Pointers we keep alive.
func defaultShards() int {
	n := runtime.GOMAXPROCS(0) * 2
	if n < 16 {
		n = 16
	}

	return nextPow2(n)
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}

	p := 1
	for p < n {
		p <<= 1
	}

	return p
}

// WithShards sets the number of shards in the subscriber map.
//
// The hot Publish path is lock-free regardless of shard count — sharding
// only reduces contention between concurrent Subscribe / Unsubscribe
// callers across many topics. The argument is rounded up to the next
// power of two. Defaults to max(16, 2*GOMAXPROCS).
func WithShards(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.shards = nextPow2(n)
		}
	}
}

// WithBufferSize toggles legacy goroutine-per-Publish async delivery.
//
// size == 0 (default) keeps Publish synchronous — the delivery loop runs
// in the caller's goroutine. Choose this for tests, low-latency paths, or
// anywhere publish ordering matters.
//
// size > 0 makes every Publish spawn a goroutine for the delivery loop.
// This is fire-and-forget: there is no bound on the number of in-flight
// goroutines and slow handlers cannot apply backpressure on the
// publisher. Always pair with Bus.Stop / StopWithContext at process exit
// so deliveries drain.
//
// For per-subscription bounded queues with proper backpressure, prefer
// SubscribeAsync over WithBufferSize.
func WithBufferSize(size int) Option {
	return func(c *config) {
		if size > 0 {
			c.bufferSize = size
		}
	}
}

// WithMiddleware registers middleware applied to every handler on every
// topic. Middleware is appended in registration order and wraps in
// reverse — the last registered is the outermost wrapper at delivery time.
//
// Panic recovery is always attached as the innermost layer; callers do
// not need to install it themselves.
func WithMiddleware(mw ...MiddlewareFunc) Option {
	return func(c *config) {
		c.middleware = append(c.middleware, mw...)
	}
}

// WithObserver registers a hook receiver for lifecycle events (publish,
// delivery start/end, drops, panics, queue depth). Pass nil to keep the
// default NoopObserver.
//
// Observers must be non-blocking and safe for concurrent use — they run
// on the publish path and inside delivery workers.
func WithObserver(o Observer) Option {
	return func(c *config) {
		if o != nil {
			c.observer = o
		}
	}
}

// WithDefaultAsyncQueueSize sets the per-worker queue capacity used by
// SubscribeAsync when the call site does not pass WithQueueSize.
// Defaults to 1024.
func WithDefaultAsyncQueueSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.asyncQueueSize = n
		}
	}
}

// WithDefaultBackpressurePolicy sets the policy used by SubscribeAsync
// when the call site does not pass WithBackpressure. Defaults to
// PolicyBlock.
func WithDefaultBackpressurePolicy(p BackpressurePolicy) Option {
	return func(c *config) {
		c.asyncPolicy = p
	}
}
