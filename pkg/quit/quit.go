// Package stop provides phased graceful shutdown for long-running services.
//
// A Quitter is a small registry where resources opt into a numbered shutdown
// phase. Lower-numbered phases run first; within a single phase every
// registered stop function executes concurrently under a shared phase
// timeout. The standard phases (PhaseHealthCheck through PhaseInfra) cover
// a typical request flow:
//
//	health-check  → load balancer drains traffic
//	transport     → gRPC/HTTP servers stop accepting, drain in-flight
//	consumer      → message consumers drain in-flight workers
//	realtime      → long-lived stream/session managers tear down
//	gateways      → business-logic gateways tear down
//	infra         → databases, event buses, external clients close
//
// Quit is idempotent and safe to call from multiple goroutines.
package stop

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

// DefaultPhaseTimeout is applied per phase when one is not configured via
// WithPhaseTimeout.
const DefaultPhaseTimeout = 15 * time.Second

// Standard shutdown phases. Lower values run first. Custom integer values
// may be used freely; the constants below describe the typical layering of
// a service.
const (
	PhaseHealthCheck = 10 // mark NOT_SERVING so the load balancer drains traffic
	PhaseTransport   = 20 // stop gRPC/HTTP servers (drain in-flight requests)
	PhaseConsumer    = 30 // stop message consumers (drain in-flight workers)
	PhaseRealtime    = 40 // stop long-lived stream/session managers
	PhaseGateways    = 50 // stop business-logic gateways
	PhaseInfra       = 60 // close DBs, event buses, external clients
)

// Quitter coordinates a phased graceful shutdown.
type Quitter interface {
	// Add registers stopFunc to run during the given phase. Lower phase
	// numbers run first; within a phase stopFuncs run concurrently. The
	// name is used for logging only. A nil stopFunc is ignored.
	Add(phase int, name string, stopFunc func() error)

	// Quit runs every registered phase in ascending order using the
	// configured phase timeout. Errors are logged. Calling Quit more than
	// once is a no-op.
	Quit()

	// QuitContext is the explicit form of Quit that returns the joined
	// set of stop errors and observes ctx for cancellation. It is
	// idempotent — subsequent calls return the result of the first.
	QuitContext(ctx context.Context) error

	// Wait blocks until ctx is canceled or one of the configured signals
	// (default SIGINT, SIGTERM) is received, then runs shutdown to
	// completion using a detached background context so that signal
	// delivery does not abort cleanup.
	Wait(ctx context.Context) error
}

// Option customises a Quitter.
type Option func(*quitter)

// WithPhaseTimeout overrides the default per-phase timeout. A value <= 0
// disables the timeout — phases run until completion.
func WithPhaseTimeout(d time.Duration) Option {
	return func(q *quitter) { q.phaseTimeout = d }
}

// WithSignals overrides the signals Wait listens for. The default set is
// SIGINT and SIGTERM. Passing no signals disables signal-based wakeup
// (Wait then unblocks only on ctx cancellation).
func WithSignals(signals ...os.Signal) Option {
	return func(q *quitter) { q.signals = signals }
}

type resource struct {
	name string
	stop func() error
}

type quitter struct {
	mu           sync.Mutex
	phases       map[int][]resource
	phaseTimeout time.Duration
	signals      []os.Signal

	once    sync.Once
	quitErr error
}

// New constructs a Quitter with optional configuration.
func New(opts ...Option) Quitter {
	q := &quitter{
		phases:       make(map[int][]resource),
		phaseTimeout: DefaultPhaseTimeout,
		signals:      []os.Signal{syscall.SIGINT, syscall.SIGTERM},
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func (q *quitter) Add(phase int, name string, stopFunc func() error) {
	if stopFunc == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.phases[phase] = append(q.phases[phase], resource{name: name, stop: stopFunc})
}

func (q *quitter) Quit() {
	_ = q.QuitContext(context.Background())
}

func (q *quitter) QuitContext(ctx context.Context) error {
	q.once.Do(func() {
		q.quitErr = q.run(ctx)
	})
	return q.quitErr
}

func (q *quitter) Wait(ctx context.Context) error {
	sigCtx, cancel := signal.NotifyContext(ctx, q.signals...)
	defer cancel()

	<-sigCtx.Done()

	cause := context.Cause(sigCtx)
	log.Info().Err(cause).Msg("shutdown signal received")

	// Detach from sigCtx so cleanup is not aborted by the very signal that
	// triggered it. Callers who want a hard ceiling on shutdown can use
	// QuitContext directly with their own context.
	return q.QuitContext(context.Background())
}

func (q *quitter) snapshot() ([]int, map[int][]resource) {
	q.mu.Lock()
	defer q.mu.Unlock()

	phases := make(map[int][]resource, len(q.phases))
	keys := make([]int, 0, len(q.phases))
	for k, v := range q.phases {
		keys = append(keys, k)
		phases[k] = slices.Clone(v)
	}
	slices.Sort(keys)
	return keys, phases
}

func (q *quitter) run(ctx context.Context) error {
	keys, phases := q.snapshot()
	if len(keys) == 0 {
		log.Info().Msg("shutdown: no resources registered")
		return nil
	}

	start := time.Now()
	var errs []error

	for _, phase := range keys {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("phase %d skipped: %w", phase, err))
			continue
		}
		errs = append(errs, q.runPhase(ctx, phase, phases[phase])...)
	}

	log.Info().Dur("elapsed", time.Since(start)).Msg("shutdown complete")
	return errors.Join(errs...)
}

func (q *quitter) runPhase(ctx context.Context, phase int, resources []resource) []error {
	if len(resources) == 0 {
		return nil
	}

	names := make([]string, len(resources))
	for i, r := range resources {
		names[i] = r.name
	}
	log.Info().
		Int("phase", phase).
		Strs("resources", names).
		Msg("shutdown phase starting")

	phaseCtx := ctx
	var cancel context.CancelFunc
	if q.phaseTimeout > 0 {
		phaseCtx, cancel = context.WithTimeout(ctx, q.phaseTimeout)
		defer cancel()
	}

	type result struct {
		name string
		err  error
	}
	ch := make(chan result, len(resources))

	for _, r := range resources {
		go func(r resource) {
			err := safeStop(r.stop)
			if err != nil {
				log.Error().
					Int("phase", phase).
					Str("resource", r.name).
					Err(err).
					Msg("resource stop failed")
			} else {
				log.Info().
					Int("phase", phase).
					Str("resource", r.name).
					Msg("resource stopped")
			}
			ch <- result{name: r.name, err: err}
		}(r)
	}

	pending := make(map[string]struct{}, len(resources))
	for _, r := range resources {
		pending[r.name] = struct{}{}
	}

	var errs []error
	for len(pending) > 0 {
		select {
		case res := <-ch:
			delete(pending, res.name)
			if res.err != nil {
				errs = append(errs, fmt.Errorf("phase %d: %s: %w", phase, res.name, res.err))
			}
		case <-phaseCtx.Done():
			outstanding := make([]string, 0, len(pending))
			for n := range pending {
				outstanding = append(outstanding, n)
			}
			slices.Sort(outstanding)

			cause := context.Cause(phaseCtx)
			log.Error().
				Int("phase", phase).
				Strs("outstanding", outstanding).
				Err(cause).
				Msg("phase did not finish in time")

			errs = append(errs, fmt.Errorf(
				"phase %d: %d resource(s) did not stop in time %v: %w",
				phase, len(outstanding), outstanding, cause,
			))

			// Stragglers' goroutines remain running; the result channel is
			// buffered so they will not block on send. Process exit takes
			// them down.
			log.Warn().Int("phase", phase).Msg("shutdown phase complete (timed out)")
			return errs
		}
	}

	log.Info().Int("phase", phase).Msg("shutdown phase complete")
	return errs
}

// safeStop converts a panic in fn into a returned error so a single bad
// resource cannot abort the entire shutdown.
func safeStop(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn()
}
