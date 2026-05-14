package eventbus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.pingocean.com/pasport/go-std/pkg/eventbus"
)

func TestBasicPubSub(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var received string
	topic := eventbus.Topic("test")

	eventbus.Subscribe(bus, topic, func(_ context.Context, msg string) {
		received = msg
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "hello")

	if received != "hello" {
		t.Errorf("expected 'hello', got %q", received)
	}
}

func TestTypeSafety(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	type Message struct{ Text string }

	var received Message
	topic := eventbus.Topic("test")

	eventbus.Subscribe(bus, topic, func(_ context.Context, msg Message) {
		received = msg
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "string")
	if received.Text != "" {
		t.Error("handler called with wrong type")
	}

	eventbus.Publish(bus, topic, Message{Text: "correct"})
	if received.Text != "correct" {
		t.Errorf("expected 'correct', got %q", received.Text)
	}
}

func TestPriority(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var order []int
	var mu sync.Mutex
	topic := eventbus.Topic("test")

	add := func(n int) func(context.Context, string) {
		return func(_ context.Context, _ string) {
			mu.Lock()
			order = append(order, n)
			mu.Unlock()
		}
	}

	eventbus.Subscribe(bus, topic, add(1), eventbus.PriorityLow)
	eventbus.Subscribe(bus, topic, add(2), eventbus.PriorityNormal)
	eventbus.Subscribe(bus, topic, add(3), eventbus.PriorityHigh)

	eventbus.Publish(bus, topic, "test")

	expected := []int{3, 2, 1}
	if len(order) != len(expected) {
		t.Fatalf("expected %d handlers, got %d", len(expected), len(order))
	}

	for i, v := range expected {
		if order[i] != v {
			t.Errorf("position %d: expected %d, got %d", i, v, order[i])
		}
	}
}

func TestAsyncMode(t *testing.T) {
	bus := eventbus.NewBus(eventbus.WithBufferSize(10))

	var count atomic.Int32
	topic := eventbus.Topic("test")

	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		time.Sleep(10 * time.Millisecond)
		count.Add(1)
	}, eventbus.PriorityNormal)

	for i := 0; i < 5; i++ {
		eventbus.Publish(bus, topic, "test")
	}

	if count.Load() > 0 {
		t.Error("async mode should not block on publish")
	}

	if err := bus.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if count.Load() != 5 {
		t.Errorf("expected 5 events processed, got %d", count.Load())
	}
}

func TestContextCancellation(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var called bool
	topic := eventbus.Topic("test")

	eventbus.Subscribe(bus, topic, func(ctx context.Context, _ string) {
		select {
		case <-ctx.Done():
			return
		default:
			called = true
		}
	}, eventbus.PriorityNormal)

	eventbus.PublishWithContext(ctx, bus, topic, "test")

	if called {
		t.Error("handler should not execute when context is cancelled")
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	bus := eventbus.NewBus(eventbus.WithMiddleware(eventbus.RecoveryMiddleware()))
	defer bus.Stop() //nolint:errcheck

	var afterPanicCalled bool
	topic := eventbus.Topic("test")

	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		panic("test panic")
	}, eventbus.PriorityHigh)

	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		afterPanicCalled = true
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "test")

	if !afterPanicCalled {
		t.Error("subsequent handler should still execute after panic")
	}
}

func TestDuplicateSubscriptionFiresTwice(t *testing.T) {
	// Registering the same handler twice yields two independent subscriptions.
	// Silent dedup would hide the more common footgun of loop-created
	// subscribers whose wrapper closures share a code address under generics.
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var count atomic.Int32
	topic := eventbus.Topic("test")

	handler := func(_ context.Context, _ string) { count.Add(1) }

	eventbus.Subscribe(bus, topic, handler, eventbus.PriorityNormal)
	eventbus.Subscribe(bus, topic, handler, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "test")

	if count.Load() != 2 {
		t.Errorf("expected handler called twice, got %d", count.Load())
	}
}

func TestUnsubscribeViaHandle(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var called bool
	topic := eventbus.Topic("test")

	handle := eventbus.Subscribe(bus, topic,
		func(_ context.Context, _ string) { called = true },
		eventbus.PriorityNormal)
	handle.Unsubscribe()

	eventbus.Publish(bus, topic, "test")

	if called {
		t.Error("unsubscribed handler should not be called")
	}
}

func TestGracefulShutdown(t *testing.T) {
	bus := eventbus.NewBus(eventbus.WithBufferSize(10))
	topic := eventbus.Topic("test")

	var completed atomic.Int32

	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		time.Sleep(50 * time.Millisecond)
		completed.Add(1)
	}, eventbus.PriorityNormal)

	for range 3 {
		eventbus.Publish(bus, topic, "test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := bus.StopWithContext(ctx); err != nil {
		t.Errorf("StopWithContext: %v", err)
	}

	if completed.Load() != 3 {
		t.Errorf("expected 3 completed, got %d", completed.Load())
	}
}

func TestShutdownTimeout(t *testing.T) {
	bus := eventbus.NewBus(eventbus.WithBufferSize(10))
	topic := eventbus.Topic("test")

	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		time.Sleep(1 * time.Second)
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "test")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := bus.StopWithContext(ctx); err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestMultipleTopics(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	var topic1Called, topic2Called bool
	topic1 := eventbus.Topic("topic1")
	topic2 := eventbus.Topic("topic2")

	eventbus.Subscribe(bus, topic1, func(_ context.Context, _ string) { topic1Called = true }, eventbus.PriorityNormal)
	eventbus.Subscribe(bus, topic2, func(_ context.Context, _ string) { topic2Called = true }, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic1, "test")

	if !topic1Called {
		t.Error("topic1 handler should be called")
	}

	if topic2Called {
		t.Error("topic2 handler should not be called")
	}
}

func TestConcurrentPublish(t *testing.T) {
	bus := eventbus.NewBus(eventbus.WithBufferSize(100))

	var count atomic.Int32
	topic := eventbus.Topic("test")

	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		count.Add(1)
	}, eventbus.PriorityNormal)

	const goroutines, eventsEach = 10, 100
	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			for range eventsEach {
				eventbus.Publish(bus, topic, "test")
			}
		})
	}

	wg.Wait()
	if err := bus.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	expected := int32(goroutines * eventsEach)
	if count.Load() != expected {
		t.Errorf("expected %d events, got %d", expected, count.Load())
	}
}

type sequencedPayload struct{ seq int64 }

func (p *sequencedPayload) AssignSeq(s int64) {
	if p.seq == 0 {
		p.seq = s
	}
}

func TestSequenceableAssigned(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	topic := eventbus.Topic("test")
	var seen []int64
	var mu sync.Mutex
	eventbus.Subscribe(bus, topic, func(_ context.Context, p *sequencedPayload) {
		mu.Lock()
		seen = append(seen, p.seq)
		mu.Unlock()
	}, eventbus.PriorityNormal)

	for range 3 {
		eventbus.Publish(bus, topic, &sequencedPayload{})
	}

	if len(seen) != 3 || seen[0] != 1 || seen[1] != 2 || seen[2] != 3 {
		t.Errorf("expected monotonic seqs [1 2 3], got %v", seen)
	}
}

func BenchmarkSyncPublish(b *testing.B) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	topic := eventbus.Topic("bench")
	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {}, eventbus.PriorityNormal)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eventbus.Publish(bus, topic, "test")
	}
}

func BenchmarkAsyncPublish(b *testing.B) {
	bus := eventbus.NewBus(eventbus.WithBufferSize(1000))

	topic := eventbus.Topic("bench")
	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {}, eventbus.PriorityNormal)

	for b.Loop() {
		eventbus.Publish(bus, topic, "test")
	}

	bus.Stop() //nolint:errcheck
}

func BenchmarkWithMiddleware(b *testing.B) {
	bus := eventbus.NewBus(eventbus.WithMiddleware(
		eventbus.RecoveryMiddleware(),
		eventbus.ContextValidationMiddleware(),
	))
	defer bus.Stop() //nolint:errcheck

	topic := eventbus.Topic("bench")
	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {}, eventbus.PriorityNormal)

	for b.Loop() {
		eventbus.Publish(bus, topic, "test")
	}
}
