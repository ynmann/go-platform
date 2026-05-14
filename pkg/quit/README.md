# stop — phased graceful shutdown

`pkg/quit` (imported as `stop`) is a tiny, dependency-light coordinator for
shutting down a Go service in **ordered phases**. Each resource registers
into a numbered phase; lower phases run first, and within a phase every
stop function executes **concurrently** under a shared timeout.

The package targets the typical lifecycle of a request-serving worker:
**stop accepting traffic → drain in-flight work → tear down infra**. Doing
this in the wrong order is one of the easiest ways to corrupt state or
return errors during a deploy.

## Why phases

A single shutdown deadline is not enough. Closing the database before
gRPC stops accepting requests yields an avalanche of 500s; closing the
HTTP server before the load balancer stops sending traffic causes brief
client errors. Phases encode the dependency order once, in one place.

```
PhaseHealthCheck (10) → mark NOT_SERVING; LB drains traffic
PhaseTransport   (20) → gRPC/HTTP stop accepting; in-flight RPCs drain
PhaseConsumer    (30) → message consumers stop pulling; workers drain
PhaseRealtime    (40) → long-lived stream/session managers tear down
PhaseGateways    (50) → business-logic gateways tear down
PhaseInfra       (60) → DBs, event bus, external clients close
```

Custom integer values are fine; the constants are just well-known names.

## Quick start

```go
package main

import (
    "context"
    "time"

    stop "git.pingocean.com/pasport/go-std/pkg/quit"
)

func main() {
    q := stop.New(stop.WithPhaseTimeout(20 * time.Second))

    // Stop accepting traffic first.
    q.Add(stop.PhaseHealthCheck, "health", health.SetNotServing)

    // Drain transport.
    q.Add(stop.PhaseTransport, "grpc", grpcSrv.Stop)
    q.Add(stop.PhaseTransport, "http", httpSrv.Stop)

    // Drain consumers.
    q.Add(stop.PhaseConsumer, "nats", consumer.Shutdown)

    // Close infra last.
    q.Add(stop.PhaseInfra, "postgres", pg.Close)
    q.Add(stop.PhaseInfra, "redis", rdb.Close)

    // Block until SIGINT/SIGTERM, then run shutdown.
    if err := q.Wait(context.Background()); err != nil {
        log.Fatal().Err(err).Msg("shutdown finished with errors")
    }
}
```

## API

### `New(opts ...Option) Quitter`

Constructs a Quitter. Defaults: `DefaultPhaseTimeout` (15s) per phase,
signals `SIGINT`+`SIGTERM`.

| Option                            | Default            | Notes |
| --------------------------------- | ------------------ | ----- |
| `WithPhaseTimeout(d)`             | `15s`              | A value `<= 0` disables timeouts. |
| `WithSignals(sigs...)`            | `SIGINT, SIGTERM`  | Empty list disables signal-based wakeup; `Wait` then unblocks only on ctx cancel. |

### `Quitter.Add(phase int, name string, stopFunc func() error)`

Registers `stopFunc` to run during `phase`. `name` is used for logging.
A `nil` stopFunc is silently ignored. Safe to call concurrently and
before/after `Quit` (additions after `Quit` has run are accepted but
will not execute, since `Quit` is one-shot).

### `Quitter.Quit()`

Convenience wrapper: `QuitContext(context.Background())`, error ignored
(it is logged). One-shot.

### `Quitter.QuitContext(ctx) error`

Runs every registered phase in ascending order. Within a phase all stop
functions run concurrently; the phase as a whole is bounded by the
configured `phaseTimeout`. Returns `errors.Join(...)` of every stop
error and timeout. **Idempotent** — subsequent calls return the cached
result of the first.

If `ctx` is canceled mid-shutdown, the current phase's timeout is
already derived from `ctx`, so it cancels the in-flight phase. Remaining
phases are skipped and reported as errors. Use this when you want a
**hard ceiling** on total shutdown time.

### `Quitter.Wait(ctx) error`

Blocks until `ctx` is canceled or one of the configured signals is
received, then runs `QuitContext` against a **detached background
context** so the very signal that triggered shutdown does not abort it.
Returns the result of `QuitContext`.

## Behavior details

- **Concurrency within a phase.** All `stopFunc`s in the same phase run in
  parallel. Two HTTP servers in `PhaseTransport` drain at the same time.
- **Sequential between phases.** A phase fully completes (or times out)
  before the next phase begins.
- **Per-phase timeout.** Each phase gets its own timeout — they do not
  share a single budget. A 6-phase shutdown with `WithPhaseTimeout(15s)`
  has a worst-case of 90s. If you need a global ceiling, pass a
  deadlined `context.Context` to `QuitContext`.
- **Timeout = stragglers logged, shutdown continues.** When a phase
  times out, the still-running goroutines are **not** waited on — the
  process moves to the next phase and lets the OS reap them on exit.
  Outstanding resource names are logged.
- **Panics are recovered.** A `panic` inside a `stopFunc` is caught and
  surfaced as an error so one bad cleanup cannot abort the rest.
- **Errors are joined.** `QuitContext` returns `errors.Join(...)` over
  every phase. Use `errors.Is` / `errors.As` to inspect.
- **Idempotent.** `Quit` / `QuitContext` is gated by `sync.Once`. Safe
  to call from multiple places (e.g. signal handler and a parent
  shutdown coordinator).
- **Logging.** Every transition (phase start, resource stop, phase
  complete, timeout) is emitted via `github.com/rs/zerolog`. The package
  produces no metrics — wrap individual `stopFunc`s if you need that.

## Patterns

### Hard global deadline

```go
ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
defer cancel()

q := stop.New(stop.WithPhaseTimeout(20 * time.Second))
// register...

<-shutdownTrigger
_ = q.QuitContext(ctx) // 60s ceiling regardless of phase count
```

### Adapting a `Close()`-only resource

`Add` wants `func() error`. Adapt with a closure:

```go
q.Add(stop.PhaseInfra, "redis", func() error {
    rdb.Close()    // returns nothing
    return nil
})

q.Add(stop.PhaseTransport, "grpc", func() error {
    grpcSrv.GracefulStop() // returns nothing, blocks
    return nil
})
```

### Tests

`Quit` is one-shot; construct a fresh `New()` per test. The package's
own tests use `WithSignals(syscall.SIGUSR1)` to avoid clobbering the
test runner with `SIGTERM`.

## Testing

```sh
go test ./pkg/quit/...
```

## What the package deliberately does not do

- **No retry of failed stops.** A failing `stopFunc` is logged and
  returned in the error chain. If retry makes sense for your resource,
  do it inside the closure.
- **No metrics.** Bring your own — wrap `stopFunc` with timing if needed.
- **No goroutine kill.** Go cannot cancel a non-cooperative goroutine.
  After a phase timeout, stragglers are left to be reaped by process
  exit; this is the same trade-off the standard library makes.
- **No “before/after” hooks.** If you need pre-shutdown work, add it to
  a lower-numbered phase.
