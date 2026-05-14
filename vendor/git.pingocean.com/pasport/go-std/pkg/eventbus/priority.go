package eventbus

// Priority controls the order in which handlers run at delivery time.
// Higher values run first; ties keep registration order.
type Priority int

const (
	// PriorityLow runs after every other handler — logging, metrics, cleanup.
	PriorityLow Priority = iota

	// PriorityNormal is the default for business logic.
	PriorityNormal

	// PriorityHigh runs before normal handlers — validation, enrichment.
	PriorityHigh

	// PriorityHighest runs before everything else — auth, gating, kill switches.
	PriorityHighest
)
