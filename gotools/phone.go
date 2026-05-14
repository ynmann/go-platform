package gotools

import "strings"

// MaskPhoneNumber returns phone with the middle characters replaced by '*',
// keeping the first head and last tail characters intact. Returns phone
// unchanged when head or tail are out of range or together span the full
// length. Operates on bytes, so safe only for ASCII (e.g. E.164 input).
func MaskPhoneNumber(phone string, head, tail int) string {
	l := len(phone)
	if head < 0 || tail < 0 || head >= l || tail >= l || head+tail >= l {
		return phone
	}

	return phone[:head] + strings.Repeat("*", l-head-tail) + phone[l-tail:]
}
