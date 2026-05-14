package ephemera

import (
	"hash/maphash"
	"sync"
	"time"
)

var hashSeed = maphash.MakeSeed()

func hashKey[K comparable](k K) uint64 {
	return maphash.Comparable[K](hashSeed, k)
}

type entry[V any] struct {
	value      V
	expiration time.Time
}

type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]entry[V]
}

func newShard[K comparable, V any](capacity int) *shard[K, V] {
	if capacity > 0 {
		return &shard[K, V]{data: make(map[K]entry[V], capacity)}
	}
	return &shard[K, V]{data: make(map[K]entry[V])}
}

func (s *shard[K, V]) set(k K, v V, exp time.Time) {
	s.mu.Lock()
	s.data[k] = entry[V]{value: v, expiration: exp}
	s.mu.Unlock()
}

func (s *shard[K, V]) get(k K, now time.Time) (V, bool) {
	s.mu.RLock()
	e, ok := s.data[k]
	s.mu.RUnlock()
	if !ok || now.After(e.expiration) {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (s *shard[K, V]) getAndTouch(k K, now time.Time, ttl time.Duration) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[k]
	if !ok || now.After(e.expiration) {
		var zero V
		return zero, false
	}
	e.expiration = now.Add(ttl)
	s.data[k] = e
	return e.value, true
}

func (s *shard[K, V]) touch(k K, now time.Time, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[k]
	if !ok || now.After(e.expiration) {
		return false
	}
	e.expiration = now.Add(ttl)
	s.data[k] = e
	return true
}

func (s *shard[K, V]) del(k K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[k]
	if ok {
		delete(s.data, k)
	}
	return e.value, ok
}

func (s *shard[K, V]) update(k K, fn func(V) V, now time.Time, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[k]
	if !ok || now.After(e.expiration) {
		return false
	}
	s.data[k] = entry[V]{value: fn(e.value), expiration: now.Add(ttl)}
	return true
}

func (s *shard[K, V]) findAndTouch(pred func(K, V) bool, now time.Time, ttl time.Duration) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.data {
		if now.After(e.expiration) {
			continue
		}
		if pred(k, e.value) {
			e.expiration = now.Add(ttl)
			s.data[k] = e
			return e.value, true
		}
	}
	var zero V
	return zero, false
}

func (s *shard[K, V]) updateWhere(pred func(K, V) bool, fn func(V) V, now time.Time, ttl time.Duration) (K, V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.data {
		if now.After(e.expiration) {
			continue
		}
		if pred(k, e.value) {
			nv := fn(e.value)
			s.data[k] = entry[V]{value: nv, expiration: now.Add(ttl)}
			return k, nv, true
		}
	}
	var zk K
	var zv V
	return zk, zv, false
}

func (s *shard[K, V]) rangeLive(now time.Time, fn func(K, V) bool) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, e := range s.data {
		if now.After(e.expiration) {
			continue
		}
		if !fn(k, e.value) {
			return false
		}
	}
	return true
}

func (s *shard[K, V]) lenLive(now time.Time) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.data {
		if !now.After(e.expiration) {
			n++
		}
	}
	return n
}

func (s *shard[K, V]) purge() {
	s.mu.Lock()
	s.data = make(map[K]entry[V])
	s.mu.Unlock()
}

func (s *shard[K, V]) sweep(now time.Time) ([]K, []V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []K
	var vals []V
	for k, e := range s.data {
		if now.After(e.expiration) {
			keys = append(keys, k)
			vals = append(vals, e.value)
			delete(s.data, k)
		}
	}
	return keys, vals
}
