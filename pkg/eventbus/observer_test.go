package eventbus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.pingocean.com/pasport/go-std/pkg/eventbus"
)

type recordingObserver struct {
	mu        sync.Mutex
	publishes int
	deliveryS int
	deliveryE int
	drops     map[eventbus.DropReason]int
	depths    int
	panics    int
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{drops: map[eventbus.DropReason]int{}}
}

func (r *recordingObserver) OnPublish(string, any) {
	r.mu.Lock()
	r.publishes++
	r.mu.Unlock()
}

func (r *recordingObserver) OnDeliveryStart(string, eventbus.SubscriptionInfo) {
	r.mu.Lock()
	r.deliveryS++
	r.mu.Unlock()
}

func (r *recordingObserver) OnDeliveryEnd(string, eventbus.SubscriptionInfo, time.Duration) {
	r.mu.Lock()
	r.deliveryE++
	r.mu.Unlock()
}

func (r *recordingObserver) OnDrop(_ string, _ any, reason eventbus.DropReason) {
	r.mu.Lock()
	r.drops[reason]++
	r.mu.Unlock()
}

func (r *recordingObserver) OnPanic(string, eventbus.SubscriptionInfo, any) {
	r.mu.Lock()
	r.panics++
	r.mu.Unlock()
}

func (r *recordingObserver) OnQueueDepth(string, eventbus.SubscriptionInfo, int, int) {
	r.mu.Lock()
	r.depths++
	r.mu.Unlock()
}

type observerSnapshot struct {
	publishes int
	deliveryS int
	deliveryE int
	drops     map[eventbus.DropReason]int
	depths    int
	panics    int
}

func (r *recordingObserver) snapshot() observerSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	cp := observerSnapshot{
		publishes: r.publishes,
		deliveryS: r.deliveryS,
		deliveryE: r.deliveryE,
		drops:     map[eventbus.DropReason]int{},
		depths:    r.depths,
		panics:    r.panics,
	}
	for k, v := range r.drops {
		cp.drops[k] = v
	}

	return cp
}

func TestObserverSyncPath(t *testing.T) {
	obs := newRecordingObserver()
	bus := eventbus.NewBus(eventbus.WithObserver(obs))
	defer bus.Stop() //nolint:errcheck

	topic := eventbus.Topic("obs")
	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {}, eventbus.PriorityNormal)

	for range 4 {
		eventbus.Publish(bus, topic, "x")
	}

	s := obs.snapshot()
	if s.publishes != 4 || s.deliveryS != 4 || s.deliveryE != 4 {
		t.Errorf("expected 4/4/4 publish/start/end, got %d/%d/%d", s.publishes, s.deliveryS, s.deliveryE)
	}
}

func TestObserverNoSubscribersDrop(t *testing.T) {
	obs := newRecordingObserver()
	bus := eventbus.NewBus(eventbus.WithObserver(obs))
	defer bus.Stop() //nolint:errcheck

	eventbus.Publish(bus, eventbus.Topic("nobody"), "x")

	s := obs.snapshot()
	if s.drops[eventbus.DropNoSubscribers] != 1 {
		t.Errorf("expected 1 DropNoSubscribers, got %d", s.drops[eventbus.DropNoSubscribers])
	}
}

func TestObserverPanic(t *testing.T) {
	obs := newRecordingObserver()
	bus := eventbus.NewBus(eventbus.WithObserver(obs))
	defer bus.Stop() //nolint:errcheck

	topic := eventbus.Topic("panicker")
	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {
		panic("boom")
	}, eventbus.PriorityNormal)

	eventbus.Publish(bus, topic, "x")

	if got := obs.snapshot().panics; got != 1 {
		t.Errorf("expected 1 panic hook, got %d", got)
	}
}

func TestObserverAfterStop(t *testing.T) {
	obs := newRecordingObserver()
	bus := eventbus.NewBus(eventbus.WithObserver(obs))

	topic := eventbus.Topic("afterstop")
	eventbus.Subscribe(bus, topic, func(_ context.Context, _ string) {}, eventbus.PriorityNormal)

	if err := bus.Stop(); err != nil {
		t.Fatal(err)
	}

	eventbus.Publish(bus, topic, "x")

	if got := obs.snapshot().drops[eventbus.DropAfterStop]; got != 1 {
		t.Errorf("expected 1 DropAfterStop, got %d", got)
	}
}

func TestObserverQueueDepth(t *testing.T) {
	obs := newRecordingObserver()
	bus := eventbus.NewBus(eventbus.WithObserver(obs))

	var done atomic.Int32
	topic := eventbus.Topic("depth")

	eventbus.SubscribeAsync(bus, topic, func(_ context.Context, _ string) {
		done.Add(1)
	}, eventbus.PriorityNormal,
		eventbus.WithQueueSize(16),
		eventbus.WithBackpressure(eventbus.PolicyBlock),
	)

	for range 8 {
		eventbus.Publish(bus, topic, "x")
	}

	if err := bus.Stop(); err != nil {
		t.Fatal(err)
	}

	if got := obs.snapshot().depths; got != 8 {
		t.Errorf("expected 8 OnQueueDepth, got %d", got)
	}
}
