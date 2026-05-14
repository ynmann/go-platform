package gotools

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
)

// ErrInvalidAESKey is returned when the supplied AES key is not 32 bytes long.
var ErrInvalidAESKey = errors.New("aes: invalid key size, expected 32 bytes")

// ErrCipherTextTooShort is returned when ciphertext is smaller than the
// AES-GCM nonce, i.e. it cannot possibly contain a valid nonce + payload.
var ErrCipherTextTooShort = errors.New("aes: ciphertext too short")

// GenerateAES256Key returns a fresh 32-byte key suitable for AES-256-GCM,
// drawn from the OS CSPRNG.
func GenerateAES256Key() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

// EncryptAES256GCM seals data with AES-256-GCM and prepends the random nonce.
// Output layout: nonce || ciphertext || auth-tag.
func EncryptAES256GCM(key, data []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidAESKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}

	ct := gcm.Seal(nil, nonce, data, nil)
	return append(nonce, ct...), nil
}

// DecryptAES256GCM opens a payload produced by EncryptAES256GCM, verifying
// its auth tag and returning the plaintext.
func DecryptAES256GCM(key, cipherData []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidAESKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(cipherData) < nonceSize {
		return nil, ErrCipherTextTooShort
	}

	nonce, ct := cipherData[:nonceSize], cipherData[nonceSize:]
	return gcm.Open(nil, nonce, ct, nil)
}

// EncryptString encrypts plaintext using AES-256-GCM with a key derived from
// secret via SHA-256, returning a base64-encoded payload (nonce || ciphertext).
// Convenient for storing or transporting encrypted strings.
//
// Note: SHA-256 is not a password KDF. For password-derived keys use argon2
// or scrypt instead of this helper.
func EncryptString(secret, plaintext string) (string, error) {
	out, err := EncryptAES256GCM(deriveAESKey(secret), []byte(plaintext))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(out), nil
}

// DecryptString reverses EncryptString: it base64-decodes the payload and
// decrypts with the SHA-256-derived key.
func DecryptString(secret, ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("aes: base64 decode: %w", err)
	}
	pt, err := DecryptAES256GCM(deriveAESKey(secret), data)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// deriveAESKey reduces an arbitrary-length secret to a 32-byte key by hashing
// it once with SHA-256.
func deriveAESKey(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}
