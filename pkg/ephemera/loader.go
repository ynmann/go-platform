package ephemera

import "sync"

type call[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

type singleflight[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*call[V]
}

func newSingleflight[K comparable, V any]() *singleflight[K, V] {
	return &singleflight[K, V]{calls: make(map[K]*call[V])}
}

func (g *singleflight[K, V]) do(key K, fn func() (V, error)) (V, error) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call[V]{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	c.wg.Done()
	return c.val, c.err
}
