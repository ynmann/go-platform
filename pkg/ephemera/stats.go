package ephemera

import "sync/atomic"

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	Hits       uint64
	Misses     uint64
	Sets       uint64
	Evictions  uint64
	Loads      uint64
	LoadErrors uint64
	Size       int
}

// HitRatio returns Hits / (Hits + Misses); zero when no lookups have happened.
func (s Stats) HitRatio() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

type counters struct {
	hits       atomic.Uint64
	misses     atomic.Uint64
	sets       atomic.Uint64
	evictions  atomic.Uint64
	loads      atomic.Uint64
	loadErrors atomic.Uint64
}

func (c *counters) snapshot(size int) Stats {
	return Stats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		Sets:       c.sets.Load(),
		Evictions:  c.evictions.Load(),
		Loads:      c.loads.Load(),
		LoadErrors: c.loadErrors.Load(),
		Size:       size,
	}
}
