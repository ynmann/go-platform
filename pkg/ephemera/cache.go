// Package ephemera is a sharded, generic, observable in-memory TTL cache.
//
// Design goals:
//   - Lock-striped storage to keep per-key operations contention-free under load.
//   - Generic over any comparable key, hashed via hash/maphash.Comparable.
//   - Built-in singleflight loader to coalesce concurrent fetches on miss.
//   - Atomic stats (hits, misses, evictions, loads) ready for /metrics scrape.
//   - Pluggable Clock for deterministic tests.
//   - Cooperative lifecycle via context.Context and *sync.WaitGroup, plus an
//     explicit Close() that is safe to call multiple times.
package ephemera

import (
	"context"
	"sync"
	"time"
)

// Loader fetches the value for a key on cache miss.
type Loader[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Cache is a sharded TTL cache with optional loader and observability hooks.
type Cache[K comparable, V any] struct {
	cfg      config
	shards   []*shard[K, V]
	mask     uint64
	loader   *singleflight[K, V]
	counters counters

	listenerMu sync.RWMutex
	onEvict    func(K, V)

	closeOnce sync.Once
	closed    chan struct{}
}

// New constructs a Cache and starts its background sweep goroutine.
// The goroutine exits when ctx is cancelled or Close is called, and signals
// completion via wg.
func New[K comparable, V any](ctx context.Context, wg *sync.WaitGroup, opts ...Option) *Cache[K, V] {
	cfg := config{
		ttl:             defaultTTL,
		cleanupInterval: defaultCleanupInterval,
		shards:          defaultShards,
		clock:           systemClock{},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.shards = roundUpToPow2(cfg.shards)

	per := 0
	if cfg.capacity > 0 {
		per = (cfg.capacity + cfg.shards - 1) / cfg.shards
	}
	shards := make([]*shard[K, V], cfg.shards)
	for i := range shards {
		shards[i] = newShard[K, V](per)
	}

	c := &Cache[K, V]{
		cfg:    cfg,
		shards: shards,
		mask:   uint64(cfg.shards - 1),
		loader: newSingleflight[K, V](),
		closed: make(chan struct{}),
	}

	wg.Add(1)
	go c.run(ctx, wg)
	return c
}

func (c *Cache[K, V]) shardFor(k K) *shard[K, V] {
	return c.shards[hashKey(k)&c.mask]
}

// Set stores value under key with the cache's default TTL.
func (c *Cache[K, V]) Set(k K, v V) {
	c.SetWithTTL(k, v, c.cfg.ttl)
}

// SetWithTTL stores value under key, expiring after ttl.
// A non-positive ttl removes the key.
func (c *Cache[K, V]) SetWithTTL(k K, v V, ttl time.Duration) {
	if ttl <= 0 {
		c.Delete(k)
		return
	}
	exp := c.cfg.clock.Now().Add(ttl)
	c.shardFor(k).set(k, v, exp)
	c.counters.sets.Add(1)
}

// Get returns the value for key if present and not expired.
func (c *Cache[K, V]) Get(k K) (V, bool) {
	v, ok := c.shardFor(k).get(k, c.cfg.clock.Now())
	if ok {
		c.counters.hits.Add(1)
	} else {
		c.counters.misses.Add(1)
	}
	return v, ok
}

// GetAndTouch returns the value and resets its expiration to the default TTL.
func (c *Cache[K, V]) GetAndTouch(k K) (V, bool) {
	v, ok := c.shardFor(k).getAndTouch(k, c.cfg.clock.Now(), c.cfg.ttl)
	if ok {
		c.counters.hits.Add(1)
	} else {
		c.counters.misses.Add(1)
	}
	return v, ok
}

// Touch resets the expiration of an existing entry. Returns false if missing or expired.
func (c *Cache[K, V]) Touch(k K) bool {
	return c.shardFor(k).touch(k, c.cfg.clock.Now(), c.cfg.ttl)
}

// Delete removes the entry. Returns true if it existed.
// OnEvict is not invoked for explicit deletes.
func (c *Cache[K, V]) Delete(k K) bool {
	_, ok := c.shardFor(k).del(k)
	return ok
}

// Update atomically replaces the value if the key is live; expiration is reset.
// Returns false if the key was missing or expired.
func (c *Cache[K, V]) Update(k K, fn func(V) V) bool {
	return c.shardFor(k).update(k, fn, c.cfg.clock.Now(), c.cfg.ttl)
}

// FindAndTouch returns the first live entry matching predicate and refreshes its TTL.
// Iteration order across shards is undefined.
func (c *Cache[K, V]) FindAndTouch(pred func(K, V) bool) (V, bool) {
	now := c.cfg.clock.Now()
	for _, s := range c.shards {
		if v, ok := s.findAndTouch(pred, now, c.cfg.ttl); ok {
			c.counters.hits.Add(1)
			return v, true
		}
	}
	c.counters.misses.Add(1)
	var zero V
	return zero, false
}

// UpdateWhere atomically updates the first live entry matching predicate.
// Returns the matched key and the post-update value.
func (c *Cache[K, V]) UpdateWhere(pred func(K, V) bool, fn func(V) V) (K, V, bool) {
	now := c.cfg.clock.Now()
	for _, s := range c.shards {
		if k, v, ok := s.updateWhere(pred, fn, now, c.cfg.ttl); ok {
			return k, v, true
		}
	}
	var zk K
	var zv V
	return zk, zv, false
}

// Range invokes fn for every live entry. Iteration stops if fn returns false.
// fn must not call back into the cache: it runs under per-shard read locks.
func (c *Cache[K, V]) Range(fn func(K, V) bool) {
	now := c.cfg.clock.Now()
	for _, s := range c.shards {
		if !s.rangeLive(now, fn) {
			return
		}
	}
}

// Len returns the count of live (non-expired) entries.
func (c *Cache[K, V]) Len() int {
	now := c.cfg.clock.Now()
	total := 0
	for _, s := range c.shards {
		total += s.lenLive(now)
	}
	return total
}

// Purge drops every entry without invoking OnEvict.
func (c *Cache[K, V]) Purge() {
	for _, s := range c.shards {
		s.purge()
	}
}

// OnEvict registers a callback invoked once per entry removed by the background
// expiration sweep. Pass nil to clear. Callbacks run outside any shard lock.
func (c *Cache[K, V]) OnEvict(fn func(K, V)) {
	c.listenerMu.Lock()
	c.onEvict = fn
	c.listenerMu.Unlock()
}

// GetOrLoad returns the cached value or, on miss, calls loader exactly once for
// concurrent callers requesting the same key. The loaded value is stored with
// the cache's default TTL.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, k K, loader Loader[K, V]) (V, error) {
	if v, ok := c.Get(k); ok {
		return v, nil
	}
	c.counters.loads.Add(1)
	v, err := c.loader.do(k, func() (V, error) {
		if v, ok := c.shardFor(k).get(k, c.cfg.clock.Now()); ok {
			return v, nil
		}
		return loader(ctx, k)
	})
	if err != nil {
		c.counters.loadErrors.Add(1)
		return v, err
	}
	c.Set(k, v)
	return v, nil
}

// Stats returns a snapshot of cache counters.
func (c *Cache[K, V]) Stats() Stats {
	return c.counters.snapshot(c.Len())
}

// Close stops the background sweep goroutine. Idempotent.
func (c *Cache[K, V]) Close() {
	c.closeOnce.Do(func() { close(c.closed) })
}

func (c *Cache[K, V]) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(c.cfg.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.sweep()
		case <-ctx.Done():
			c.Purge()
			return
		case <-c.closed:
			return
		}
	}
}

func (c *Cache[K, V]) sweep() {
	now := c.cfg.clock.Now()
	c.listenerMu.RLock()
	cb := c.onEvict
	c.listenerMu.RUnlock()
	for _, s := range c.shards {
		ks, vs := s.sweep(now)
		if len(ks) == 0 {
			continue
		}
		c.counters.evictions.Add(uint64(len(ks)))
		if cb == nil {
			continue
		}
		for i, k := range ks {
			cb(k, vs[i])
		}
	}
}

func roundUpToPow2(n int) int {
	if n < 1 {
		return 1
	}
	v := uint64(n - 1)
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return int(v + 1)
}
