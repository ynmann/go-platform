package stop

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	// Silence package-level logging during tests.
	log.Logger = zerolog.New(io.Discard)
	m.Run()
}

func TestPhasesRunInAscendingOrder(t *testing.T) {
	t.Parallel()

	q := New()

	var (
		mu    sync.Mutex
		order []int
	)
	record := func(phase int) func() error {
		return func() error {
			mu.Lock()
			order = append(order, phase)
			mu.Unlock()
			return nil
		}
	}

	q.Add(PhaseInfra, "infra", record(PhaseInfra))
	q.Add(PhaseTransport, "transport", record(PhaseTransport))
	q.Add(PhaseHealthCheck, "health", record(PhaseHealthCheck))
	q.Add(PhaseGateways, "gateways", record(PhaseGateways))

	if err := q.QuitContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []int{PhaseHealthCheck, PhaseTransport, PhaseGateways, PhaseInfra}
	if len(order) != len(want) {
		t.Fatalf("expected %d phases, got %d (%v)", len(want), len(order), order)
	}
	for i, p := range want {
		if order[i] != p {
			t.Fatalf("at index %d expected phase %d, got %d (full=%v)", i, p, order[i], order)
		}
	}
}

func TestResourcesWithinPhaseRunConcurrently(t *testing.T) {
	t.Parallel()

	const n = 5
	q := New(WithPhaseTimeout(time.Second))

	var inFlight int32
	var maxInFlight int32

	for i := 0; i < n; i++ {
		q.Add(PhaseTransport, "r", func() error {
			cur := atomic.AddInt32(&inFlight, 1)
			defer atomic.AddInt32(&inFlight, -1)
			for {
				prev := atomic.LoadInt32(&maxInFlight)
				if cur <= prev || atomic.CompareAndSwapInt32(&maxInFlight, prev, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}

	q.Quit()

	if got := atomic.LoadInt32(&maxInFlight); got < 2 {
		t.Fatalf("expected concurrent execution, max inflight was %d", got)
	}
}

func TestQuitIsIdempotent(t *testing.T) {
	t.Parallel()

	q := New()
	var calls int32
	q.Add(PhaseInfra, "once", func() error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	q.Quit()
	q.Quit()
	if err := q.QuitContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected stop to be called exactly once, got %d", got)
	}
}

func TestStopErrorsAreJoined(t *testing.T) {
	t.Parallel()

	errA := errors.New("a failed")
	errB := errors.New("b failed")

	q := New()
	q.Add(PhaseTransport, "a", func() error { return errA })
	q.Add(PhaseTransport, "b", func() error { return errB })
	q.Add(PhaseInfra, "c", func() error { return nil })

	err := q.QuitContext(context.Background())
	if err == nil {
		t.Fatal("expected joined error, got nil")
	}
	if !errors.Is(err, errA) {
		t.Errorf("expected error chain to wrap errA, got %v", err)
	}
	if !errors.Is(err, errB) {
		t.Errorf("expected error chain to wrap errB, got %v", err)
	}
}

func TestPhaseTimeoutReportsStragglers(t *testing.T) {
	t.Parallel()

	q := New(WithPhaseTimeout(50 * time.Millisecond))
	q.Add(PhaseTransport, "fast", func() error { return nil })
	q.Add(PhaseTransport, "slow", func() error {
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	start := time.Now()
	err := q.QuitContext(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("Quit blocked too long: %v (expected ~50ms)", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded in chain, got %v", err)
	}
}

func TestPanicInStopIsRecovered(t *testing.T) {
	t.Parallel()

	q := New()
	q.Add(PhaseTransport, "panicker", func() error {
		panic("boom")
	})
	q.Add(PhaseInfra, "after", func() error { return nil })

	err := q.QuitContext(context.Background())
	if err == nil {
		t.Fatal("expected error from panicking stopFunc")
	}
}

func TestContextCancellationSkipsRemainingPhases(t *testing.T) {
	t.Parallel()

	q := New(WithPhaseTimeout(time.Second))

	var laterCalled atomic.Bool
	q.Add(PhaseTransport, "first", func() error { return nil })
	q.Add(PhaseInfra, "later", func() error {
		laterCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := q.QuitContext(ctx)
	if err == nil {
		t.Fatal("expected context cancellation to surface as error")
	}
	if laterCalled.Load() {
		t.Error("expected later phase to be skipped after ctx cancel")
	}
}

func TestNilStopFuncIsIgnored(t *testing.T) {
	t.Parallel()

	q := New()
	q.Add(PhaseInfra, "nil-fn", nil)

	if err := q.QuitContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEmptyQuitterIsNoOp(t *testing.T) {
	t.Parallel()

	if err := New().QuitContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitTriggersOnSignal(t *testing.T) {
	t.Parallel()

	q := New(
		WithPhaseTimeout(time.Second),
		WithSignals(syscall.SIGUSR1),
	)
	var stopped atomic.Bool
	q.Add(PhaseInfra, "infra", func() error {
		stopped.Store(true)
		return nil
	})

	done := make(chan error, 1)
	go func() {
		done <- q.Wait(context.Background())
	}()

	// Give Wait time to install the signal handler.
	time.Sleep(20 * time.Millisecond)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after signal")
	}

	if !stopped.Load() {
		t.Fatal("expected stopFunc to run on signal-triggered shutdown")
	}
}

func TestWaitTriggersOnContextCancel(t *testing.T) {
	t.Parallel()

	q := New(WithPhaseTimeout(time.Second))
	var stopped atomic.Bool
	q.Add(PhaseInfra, "infra", func() error {
		stopped.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- q.Wait(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after ctx cancel")
	}

	if !stopped.Load() {
		t.Fatal("expected stopFunc to run on ctx-triggered shutdown")
	}
}

func TestConcurrentAddIsSafe(t *testing.T) {
	t.Parallel()

	q := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.Add(PhaseTransport, "x", func() error { return nil })
		}()
	}
	wg.Wait()

	if err := q.QuitContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
