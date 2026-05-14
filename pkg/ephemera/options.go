package ephemera

import "time"

const (
	defaultTTL             = 5 * time.Minute
	defaultCleanupInterval = time.Minute
	defaultShards          = 16
)

type config struct {
	ttl             time.Duration
	cleanupInterval time.Duration
	shards          int
	capacity        int
	clock           Clock
}

// Option configures a Cache at construction time.
type Option func(*config)

// WithTTL sets the default time-to-live for entries inserted via Set.
func WithTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.ttl = d
		}
	}
}

// WithCleanupInterval controls how often expired entries are swept.
func WithCleanupInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.cleanupInterval = d
		}
	}
}

// WithShards sets the number of lock-striped shards. Rounded up to the next power of two.
func WithShards(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.shards = n
		}
	}
}

// WithCapacity hints the total number of expected live entries; distributed across shards.
func WithCapacity(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.capacity = n
		}
	}
}

// WithClock injects a Clock for deterministic tests.
func WithClock(clock Clock) Option {
	return func(c *config) {
		if clock != nil {
			c.clock = clock
		}
	}
}
