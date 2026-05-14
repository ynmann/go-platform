package gotools

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashString returns the hex-encoded SHA-256 hash of input. Useful for
// deterministic cache keys and short content fingerprints. Not a password
// hash — use bcrypt/argon2 for credential storage.
func HashString(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}
