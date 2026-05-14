package gotools

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// AsTime safely converts proto Timestamp to time.Time.
// Returns zero time if ts is nil or invalid.
func AsTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}

	if err := ts.CheckValid(); err != nil {
		return time.Time{}
	}

	return ts.AsTime()
}

// PtrAsTime returns *time.Time or nil.
func PtrAsTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}

	if err := ts.CheckValid(); err != nil {
		return nil
	}

	t := ts.AsTime()
	if t.IsZero() {
		return nil
	}

	return &t
}

// FromTime converts time.Time to proto Timestamp.
// Zero time → nil.
func FromTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}

	return timestamppb.New(t)
}
