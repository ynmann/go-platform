package eventbus

import "hash/fnv"

// Partitioned is implemented by payloads that should be routed to a specific
// async worker within a subscription. All payloads with the same
// PartitionKey reach the same worker, which preserves per-key ordering
// while allowing parallel delivery for unrelated keys.
//
// A subscription opts into multi-worker delivery via WithPartitionedWorkers.
// When the worker count is 1 (the default), PartitionKey is ignored and
// strict ordering across the entire subscription is preserved instead.
type Partitioned interface {
	PartitionKey() string
}

// partitionIndex hashes key onto [0, workers).
func partitionIndex(key string, workers int) int {
	if workers <= 1 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return int(h.Sum32() % uint32(workers))
}
