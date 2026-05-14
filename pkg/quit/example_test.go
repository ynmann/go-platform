package stop_test

import (
	"context"
	"time"

	stop "git.pingocean.com/pasport/go-std/pkg/quit"
)

// Typical worker entrypoint: register resources into phases, then block
// on Wait until the orchestrator delivers SIGTERM.
func ExampleQuitter_Wait() {
	type server interface{ Stop() error }
	type db interface{ Close() error }

	var (
		grpcSrv server
		httpSrv server
		pg      db
	)

	q := stop.New(stop.WithPhaseTimeout(20 * time.Second))

	q.Add(stop.PhaseTransport, "grpc", grpcSrv.Stop)
	q.Add(stop.PhaseTransport, "http", httpSrv.Stop)
	q.Add(stop.PhaseInfra, "postgres", pg.Close)

	// Blocks until SIGINT/SIGTERM, then drains every phase.
	_ = q.Wait(context.Background())
}

// Tests and short-lived programs can drive shutdown explicitly.
func ExampleQuitter_QuitContext() {
	q := stop.New()
	q.Add(stop.PhaseInfra, "cache", func() error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = q.QuitContext(ctx)
}
