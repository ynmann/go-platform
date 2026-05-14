package eventbus_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.pingocean.com/pasport/go-std/pkg/eventbus"
)

func TestSubscribeAsyncBasic(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var count atomic.Int32
	topic := eventbus.Topic("async")

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		count.Add(1)
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(8),
		eventbus.WithBackpressure(eventbus.PolicyBlock),
	)

	for range 5 {
		eventbus.Publish(bus, topic, "x")
	}

	if err := bus.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if count.Load() != 5 {
		t.Errorf("expected 5 deliveries, got %d", count.Load())
	}
}

func TestSubscribeAsyncDropNewest(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	gate := make(chan struct{})
	released := make(chan struct{})
	var processed atomic.Int32
	topic := eventbus.Topic("drop_newest")

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		<-gate

		processed.Add(1)
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(2),
		eventbus.WithBackpressure(eventbus.PolicyDropNewest),
	)

	// First payload occupies the worker; next two fill the queue; the
	// remainder must be dropped without blocking.
	for i := range 10 {
		eventbus.Publish(bus, topic, fmt.Sprintf("msg-%d", i))
	}

	close(gate)

	go func() {
		_ = bus.Stop()

		close(released)
	}()

	select {
	case <-released:

	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not finish — drop policy should not have blocked")
	}

	if got := processed.Load(); got > 3 || got < 1 {
		t.Errorf("expected 1..3 deliveries with drop-newest, got %d", got)
	}
}

func TestSubscribeAsyncDropOldest(t *testing.T) {
	bus := eventbus.NewBus()

	gate := make(chan struct{})
	var processed atomic.Int32
	topic := eventbus.Topic("drop_oldest")

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		<-gate

		processed.Add(1)
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(2),
		eventbus.WithBackpressure(eventbus.PolicyDropOldest),
	)

	for i := range 10 {
		eventbus.Publish(bus, topic, fmt.Sprintf("msg-%d", i))
	}

	close(gate)

	if err := bus.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if got := processed.Load(); got < 1 {
		t.Errorf("expected at least 1 delivery, got %d", got)
	}
}

func TestSubscribeAsyncBlockBackpressure(t *testing.T) {
	bus := eventbus.NewBus()

	var processed atomic.Int32
	topic := eventbus.Topic("block")

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		time.Sleep(5 * time.Millisecond)

		processed.Add(1)
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(2),
		eventbus.WithBackpressure(eventbus.PolicyBlock),
	)

	for range 50 {
		eventbus.Publish(bus, topic, "x")
	}

	if err := bus.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if processed.Load() != 50 {
		t.Errorf("PolicyBlock must deliver every payload, got %d", processed.Load())
	}
}

type partKeyed struct {
	key  string
	body int
}

func (p partKeyed) PartitionKey() string {
	return p.key
}

func TestSubscribeAsyncPartitionedOrdering(t *testing.T) {
	bus := eventbus.NewBus()

	const keys, perKey = 8, 50

	type record struct {
		body int
	}

	var (
		mu       sync.Mutex
		observed = map[string][]int{}
	)

	topic := eventbus.Topic("part")

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, p partKeyed) {
		// Adding tiny jitter increases the chance that, without proper
		// partition routing, two payloads of the same key would be reordered.
		time.Sleep(time.Microsecond)

		mu.Lock()
		observed[p.key] = append(observed[p.key], p.body)
		mu.Unlock()
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(64),
		eventbus.WithPartitionedWorkers(4),
	)

	for body := range perKey {
		for k := range keys {
			eventbus.Publish(bus, topic, partKeyed{key: fmt.Sprintf("k-%d", k), body: body})
		}
	}

	if err := bus.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(observed) != keys {
		t.Fatalf("expected %d keys, got %d", keys, len(observed))
	}

	for k, seq := range observed {
		if len(seq) != perKey {
			t.Errorf("key %s: expected %d events, got %d", k, perKey, len(seq))

			continue
		}

		for i, v := range seq {
			if v != i {
				t.Errorf("key %s: out-of-order at index %d (got %d, want %d)", k, i, v, i)

				break
			}
		}
	}
}

func TestSubscribeAsyncSlowConsumerIsolation(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var fastCount atomic.Int32
	slowGate := make(chan struct{})
	topic := eventbus.Topic("isolation")

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		<-slowGate
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(2),
		eventbus.WithBackpressure(eventbus.PolicyDropNewest),
	)

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		fastCount.Add(1)
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(64),
		eventbus.WithBackpressure(eventbus.PolicyBlock),
	)

	for range 32 {
		eventbus.Publish(bus, topic, "x")
	}

	deadline := time.Now().Add(2 * time.Second)
	for fastCount.Load() < 32 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	if fastCount.Load() != 32 {
		t.Errorf("fast subscriber should not have been blocked by slow peer, got %d/32", fastCount.Load())
	}

	close(slowGate)
}

func TestUnsubscribeAsyncCleansUp(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var count atomic.Int32
	topic := eventbus.Topic("unsub_async")

	handle := eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		count.Add(1)
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "first")

	deadline := time.Now().Add(time.Second)
	for count.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	handle.Unsubscribe()

	// Subscribe a second handler so deliver finds someone and skips the
	// "no subscribers" warning path. The first (unsubscribed) handler must
	// not fire.
	var second atomic.Int32

	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		second.Add(1)
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "second")

	if second.Load() != 1 {
		t.Errorf("replacement subscriber should fire once, got %d", second.Load())
	}

	if count.Load() != 1 {
		t.Errorf("unsubscribed async handler must not fire again, got %d", count.Load())
	}
}
