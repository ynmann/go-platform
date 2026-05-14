package ephemera

import "time"

// Clock abstracts time.Now for deterministic tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
