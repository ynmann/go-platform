package gotools

import "fmt"

// EPretty wraps err with a "[funcName]" prefix for log/trace readability.
// Returns nil when err is nil so callers can use it inline at return sites.
func EPretty(funcName string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("[%s] %w", funcName, err)
}

// Join2Errors joins two named errors, skipping any nil ones. Each non-nil
// error is prefixed with its name via EPretty. Returns nil when both are nil.
func Join2Errors(name1, name2 string, err1, err2 error) error {
	switch {
	case err1 == nil && err2 == nil:
		return nil
	case err1 == nil:
		return EPretty(name2, err2)
	case err2 == nil:
		return EPretty(name1, err1)
	default:
		return fmt.Errorf("%w; %w", EPretty(name1, err1), EPretty(name2, err2))
	}
}
