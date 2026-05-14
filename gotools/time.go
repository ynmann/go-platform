package gotools

import (
	"fmt"
	"slices"
	"time"
)

// FormatDuration prints d as "HH:MM:SS". The hour component is uncapped, so
// durations longer than 24 hours render as e.g. "73:02:15".
func FormatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

// EarliestDate returns the chronologically earliest time in dates, or the
// zero time when dates is empty. The input is sorted in place.
func EarliestDate(dates ...time.Time) time.Time {
	if len(dates) == 0 {
		return time.Time{}
	}
	slices.SortFunc(dates, func(a, b time.Time) int { return a.Compare(b) })
	return dates[0]
}

// LatestDate returns the chronologically latest time in dates, or the zero
// time when dates is empty. The input is sorted in place.
func LatestDate(dates ...time.Time) time.Time {
	if len(dates) == 0 {
		return time.Time{}
	}
	slices.SortFunc(dates, func(a, b time.Time) int { return a.Compare(b) })
	return dates[len(dates)-1]
}

// TimeFromUnix converts a Unix-seconds timestamp to time.Time. A zero ts
// returns the zero time, so callers can distinguish "unset" from epoch.
func TimeFromUnix(ts int64) time.Time {
	if ts == 0 {
		return time.Time{}
	}
	return time.Unix(ts, 0)
}

// TimeFromUnixPtr returns *time.Time for ts, or nil when ts == 0.
func TimeFromUnixPtr(ts int64) *time.Time {
	if ts == 0 {
		return nil
	}
	t := time.Unix(ts, 0)
	return &t
}

// FlexTime is a time.Time that unmarshals from JSON in any of:
//   - RFC3339 / RFC3339Nano (with timezone),
//   - "2006-01-02T15:04:05[.fractional]" (no timezone, the format that
//     PostgreSQL TIMESTAMP without time zone serializes to).
//
// Marshalling always produces RFC3339Nano in UTC.
type FlexTime struct {
	time.Time
}

var flexTimeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
}

// UnmarshalJSON parses b into t, accepting the formats listed on FlexTime.
func (t *FlexTime) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	s := string(b)
	if len(s) >= 2 && s[0] == '"' {
		s = s[1 : len(s)-1]
	}
	for _, f := range flexTimeFormats {
		if parsed, err := time.Parse(f, s); err == nil {
			t.Time = parsed
			return nil
		}
	}
	return fmt.Errorf("FlexTime: cannot parse %q", s)
}

// MarshalJSON renders t as an RFC3339Nano UTC string.
func (t FlexTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.UTC().Format(time.RFC3339Nano) + `"`), nil
}
