package eventbus

import (
	"sort"
	"sync"
	"sync/atomic"
)

// shard is a slice of the topic→subscribers index. Every shard owns a
// sync.Map whose values are *atomic.Pointer[[]*subscription] — that is,
// the per-topic subscriber list is held behind an atomic pointer.
//
// The Publish hot path is therefore lock-free:
//
//	v, _ := shard.subs.Load(topic)
//	list := v.(*atomic.Pointer[[]*subscription]).Load()
//
// Subscribe / Unsubscribe build a new sorted slice and CAS it into the
// pointer. Concurrent writers retry on CAS failure. The slice is sorted
// once at write time so the delivery loop does not need to sort on every
// Publish.
//
// We never delete the *atomic.Pointer entry from the sync.Map even when
// its slice becomes empty — the next Subscribe on the same topic reuses
// it, avoiding allocator and sync.Map churn.
type shard struct {
	subs sync.Map // string → *atomic.Pointer[[]*subscription]
}

func newShard() *shard {
	return &shard{}
}

// load returns the current snapshot of subscribers for topic, or nil if
// the topic has never been subscribed to. The returned slice is read-only
// — callers must not mutate it.
func (s *shard) load(topic string) []*subscription {
	v, ok := s.subs.Load(topic)
	if !ok {
		return nil
	}

	p := v.(*atomic.Pointer[[]*subscription])

	out := p.Load()
	if out == nil {
		return nil
	}

	return *out
}

func (s *shard) addSub(topic string, sub *subscription) {
	v, _ := s.subs.LoadOrStore(topic, &atomic.Pointer[[]*subscription]{})
	p := v.(*atomic.Pointer[[]*subscription])

	for {
		old := p.Load()

		var newSlice []*subscription
		if old == nil {
			newSlice = []*subscription{sub}
		} else {
			newSlice = make([]*subscription, len(*old)+1)
			copy(newSlice, *old)
			newSlice[len(*old)] = sub
		}

		sortByPriority(newSlice)

		if p.CompareAndSwap(old, &newSlice) {
			return
		}
	}
}

func (s *shard) removeSub(topic string, target *subscription) bool {
	v, ok := s.subs.Load(topic)
	if !ok {
		return false
	}

	p := v.(*atomic.Pointer[[]*subscription])

	for {
		old := p.Load()
		if old == nil {
			return false
		}

		filtered := make([]*subscription, 0, len(*old))
		for _, s := range *old {
			if s != target {
				filtered = append(filtered, s)
			}
		}

		if len(filtered) == len(*old) {
			return false
		}

		if p.CompareAndSwap(old, &filtered) {
			return true
		}
	}
}

// rangeAll iterates every subscription across every topic in the shard.
// Used by Bus.Stop to signal async subscriptions to drain.
func (s *shard) rangeAll(fn func(topic string, sub *subscription)) {
	s.subs.Range(func(k, v any) bool {
		topic := k.(string)
		p := v.(*atomic.Pointer[[]*subscription])

		list := p.Load()
		if list == nil {
			return true
		}

		for _, sub := range *list {
			fn(topic, sub)
		}

		return true
	})
}

func sortByPriority(subs []*subscription) {
	sort.SliceStable(subs, func(i, j int) bool {
		return subs[i].priority > subs[j].priority
	})
}
