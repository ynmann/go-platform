package gotools

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"slices"
	"strings"
	"time"
)

var (
	// ErrInvalidCertFormat indicates a PEM block is missing or not a certificate.
	ErrInvalidCertFormat = errors.New("invalid certificate format")

	// ErrInvalidKeyFormat indicates a PEM block is missing or is not a
	// recognized private-key encoding (PKCS#1, PKCS#8 or EC SEC1).
	ErrInvalidKeyFormat = errors.New("invalid private key format")

	// ErrCertExpired indicates the certificate's NotAfter is in the past.
	ErrCertExpired = errors.New("certificate expired")

	// ErrCertNotYetValid indicates the certificate's NotBefore is in the future.
	ErrCertNotYetValid = errors.New("certificate not yet valid")
)

// ParseCertificate decodes a PEM-encoded X.509 certificate and verifies the
// validity window plus that the public key algorithm is recognized.
func ParseCertificate(pemData []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemData)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, ErrInvalidCertFormat
	}

	crt, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, ErrInvalidCertFormat
	}

	now := time.Now()
	if now.After(crt.NotAfter) {
		return nil, ErrCertExpired
	}
	if now.Before(crt.NotBefore) {
		return nil, ErrCertNotYetValid
	}
	if crt.PublicKeyAlgorithm == x509.UnknownPublicKeyAlgorithm {
		return nil, ErrInvalidCertFormat
	}

	return crt, nil
}

// ParseCertificateFromBytes decodes a PEM certificate and returns it together
// with its NotAfter (expiration) timestamp. Unlike ParseCertificate it does
// not verify the validity window, which is useful for monitoring.
func ParseCertificateFromBytes(pemData []byte) (*x509.Certificate, time.Time, error) {
	block, _ := pem.Decode(pemData)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, time.Time{}, ErrInvalidCertFormat
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, time.Time{}, ErrInvalidCertFormat
	}
	return cert, cert.NotAfter, nil
}

// ParsePrivateKey decodes a PEM-encoded private key, attempting PKCS#1, PKCS#8
// and EC SEC1 formats in that order. Returns the concrete *rsa.PrivateKey,
// *ecdsa.PrivateKey or ed25519.PrivateKey.
func ParsePrivateKey(pemData []byte) (any, error) {
	block, _ := pem.Decode(pemData)
	if block == nil || !strings.HasSuffix(block.Type, "PRIVATE KEY") {
		return nil, ErrInvalidKeyFormat
	}

	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	if k, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return k, nil
	}

	return nil, ErrInvalidKeyFormat
}

// GetCertExpiration parses a PEM certificate and returns its NotAfter.
func GetCertExpiration(pemData []byte) (time.Time, error) {
	if len(pemData) == 0 {
		return time.Time{}, ErrInvalidCertFormat
	}
	block, _ := pem.Decode(pemData)
	if block == nil || block.Type != "CERTIFICATE" {
		return time.Time{}, ErrInvalidCertFormat
	}
	crt, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, ErrInvalidCertFormat
	}
	return crt.NotAfter, nil
}

// KeyMatchesCertificate reports whether certPubKey was produced by privKey.
// Supports RSA, ECDSA and Ed25519. The error result is non-nil only when the
// key types are unsupported or do not match each other.
func KeyMatchesCertificate(certPubKey any, privKey crypto.PrivateKey) (bool, error) {
	signer, ok := privKey.(interface{ Public() crypto.PublicKey })
	if !ok {
		return false, errors.New("private key does not implement Public() crypto.PublicKey")
	}

	switch expected := signer.Public().(type) {
	case *rsa.PublicKey:
		if certPub, ok := certPubKey.(*rsa.PublicKey); ok {
			return expected.N.Cmp(certPub.N) == 0 && expected.E == certPub.E, nil
		}
	case *ecdsa.PublicKey:
		if certPub, ok := certPubKey.(*ecdsa.PublicKey); ok {
			return expected.X.Cmp(certPub.X) == 0 && expected.Y.Cmp(certPub.Y) == 0, nil
		}
	case ed25519.PublicKey:
		if certPub, ok := certPubKey.(ed25519.PublicKey); ok {
			return expected.Equal(certPub), nil
		}
	default:
		return false, errors.New("unsupported public key type")
	}

	return false, errors.New("public key type mismatch between certificate and private key")
}

// CertMatchesIP reports whether the certificate's SAN list contains the given
// IP literal, falling back to a DNSNames lookup so callers that store
// hostnames in IP fields still match.
func CertMatchesIP(cert *x509.Certificate, ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, sanIP := range cert.IPAddresses {
		if sanIP.Equal(parsed) {
			return true
		}
	}
	return slices.Contains(cert.DNSNames, ip)
}
