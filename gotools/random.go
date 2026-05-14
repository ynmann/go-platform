package gotools

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand/v2"
	"strings"
)

// CharsetAlphanumeric is the lower- and upper-case ASCII alphanumeric
// alphabet (62 symbols), suitable for short tokens that need to look
// human-readable.
const CharsetAlphanumeric = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// CharsetDigits contains decimal digits 0-9.
const CharsetDigits = "0123456789"

// RandInt returns a non-cryptographic pseudo-random int in [0, n). Backed by
// math/rand/v2's concurrency-safe global source. Do not use for security-
// sensitive randomness — see CryptoRandomString instead.
func RandInt(n int) int { return mrand.IntN(n) }

// RandomString returns a non-cryptographic alphanumeric string of length n.
// Use CryptoRandomString for tokens that must resist guessing.
func RandomString(n int) string {
	if n <= 0 {
		return ""
	}
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte(CharsetAlphanumeric[mrand.IntN(len(CharsetAlphanumeric))])
	}
	return sb.String()
}

// CryptoRandomString returns a string of length n drawn uniformly from the
// alphanumeric charset using crypto/rand.
func CryptoRandomString(n int) (string, error) {
	return cryptoRandomFromCharset(n, CharsetAlphanumeric)
}

// CryptoRandomDigitsString returns a string of n decimal digits using
// crypto/rand. Suitable for short verification codes (PIN, OTP).
func CryptoRandomDigitsString(n int) (string, error) {
	return cryptoRandomFromCharset(n, CharsetDigits)
}

// CryptoRandomBytes returns n cryptographically random bytes.
func CryptoRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// GenerateCode returns a string of n decimal digits using a non-cryptographic
// source. For verification codes prefer CryptoRandomDigitsString.
func GenerateCode(n int) string {
	if n <= 0 {
		return ""
	}
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte('0' + byte(mrand.IntN(10)))
	}
	return sb.String()
}

// IsAllRunesSame reports whether s consists of a single repeated byte. An
// empty string returns true. Operates on bytes, not runes — safe for ASCII.
func IsAllRunesSame(s string) bool {
	if len(s) == 0 {
		return true
	}
	first := s[0]
	for i := 1; i < len(s); i++ {
		if s[i] != first {
			return false
		}
	}
	return true
}

func cryptoRandomFromCharset(n int, charset string) (string, error) {
	if n <= 0 {
		return "", nil
	}
	out := make([]byte, n)
	max := big.NewInt(int64(len(charset)))
	for i := range out {
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		out[i] = charset[idx.Int64()]
	}
	return string(out), nil
}
