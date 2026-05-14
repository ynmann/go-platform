package gotools

import (
	"runtime"

	"github.com/rs/zerolog/log"
)

// LogMemUsage emits a single info log with current heap allocations, total
// allocations since process start, system memory, GC count and active
// goroutines. stage is a free-form label written under the "stage" field —
// useful to bookend a hot section ("before-import", "after-import").
func LogMemUsage(stage string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Info().
		Str("stage", stage).
		Uint64("alloc_mb", m.Alloc/1024/1024).
		Uint64("total_alloc_mb", m.TotalAlloc/1024/1024).
		Uint64("sys_mb", m.Sys/1024/1024).
		Uint32("num_gc", m.NumGC).
		Int("goroutines", runtime.NumGoroutine()).
		Msg("[mem] usage")
}
