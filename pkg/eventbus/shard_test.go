package eventbus_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"git.pingocean.com/pasport/go-std/pkg/eventbus"
)

func TestShardingDistributesTopics(t *testing.T) {
	bus := eventbus.NewBus(eventbus.WithShards(64))
	defer bus.Stop() //nolint:errcheck

	const topics = 256

	var hits atomic.Int32

	for i := range topics {
		topic := eventbus.Topic(fmt.Sprintf("t-%d", i))
		eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
			hits.Add(1)
		}, eventbus.PriorityNormal)
	}

	var wg sync.WaitGroup
	for i := range topics {
		wg.Go(func() {
			eventbus.Publish(bus, eventbus.Topic(fmt.Sprintf("t-%d", i)), "x")
		})
	}

	wg.Wait()

	if hits.Load() != topics {
		t.Errorf("expected %d hits, got %d", topics, hits.Load())
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	bus := eventbus.NewBus()
	defer bus.Stop() //nolint:errcheck

	const writers, churnEach = 8, 200

	topic := eventbus.Topic("churn")
	var wg sync.WaitGroup

	for range writers {
		wg.Go(func() {
			for range churnEach {
				h := eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {}, eventbus.PriorityNormal)
				eventbus.Publish(bus, topic, "x")
				h.Unsubscribe()
			}
		})
	}

	wg.Wait()
}

func TestShardsRoundsToPowerOfTwo(t *testing.T) {
	// Sanity: WithShards(33) must not panic and must produce a usable bus.
	bus := eventbus.NewBus(eventbus.WithShards(33))
	defer bus.Stop() //nolint:errcheck

	var got string
	topic := eventbus.Topic("t")

	eventbus.Subscribe(bus, topic, func(_ context.Context, msg string) {
		got = msg
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "ok")

	if got != "ok" {
		t.Errorf("expected 'ok', got %q", got)
	}
}
