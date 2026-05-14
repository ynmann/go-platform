package gotools

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// rsaCertTemplate is the X.509 template shared by certificates produced by
// GenerateRSAPair. Subject and SAN are deliberately generic; replace at the
// call site if you need a specific identity.
var rsaCertTemplate = x509.Certificate{
	SerialNumber: big.NewInt(1),
	Subject: pkix.Name{
		Organization: []string{"Example"},
		CommonName:   "example",
	},
	KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	BasicConstraintsValid: true,
	DNSNames:              []string{"example"},
}

// GenerateRSAPair creates a fresh RSA key pair of the requested bit length
// and returns the PKCS#1 private key plus a one-year self-signed certificate,
// both PEM-encoded. Suitable for bootstrapping internal mutual-TLS pairs.
func GenerateRSAPair(bits uint16) (privatePEM, certPEM string, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, int(bits))
	if err != nil {
		return "", "", err
	}

	tmpl := rsaCertTemplate
	tmpl.NotBefore = time.Now().UTC()
	tmpl.NotAfter = tmpl.NotBefore.Add(365 * 24 * time.Hour)

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, crypto.Signer(priv))
	if err != nil {
		return "", "", err
	}

	privatePEM = string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(priv),
	}))
	certPEM = string(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	}))
	return privatePEM, certPEM, nil
}

// EncryptAESKeyWithRSAOAEP wraps a symmetric AES key under the RSA public key
// found in certPEM, using OAEP-SHA256. Output is the raw RSA ciphertext.
// Typical use: a client encrypts a freshly generated AES key for a server it
// only knows by certificate.
func EncryptAESKeyWithRSAOAEP(certPEM string, aesKey []byte) ([]byte, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, ErrInvalidCertFormat
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("rsa: parse certificate: %w", err)
	}
	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("rsa: certificate does not contain an RSA public key")
	}
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, aesKey, nil)
}

// DecryptAESKeyWithRSAOAEP unwraps a key produced by EncryptAESKeyWithRSAOAEP
// using a PEM-encoded RSA private key (PKCS#1 or PKCS#8).
func DecryptAESKeyWithRSAOAEP(privateKeyPEM string, ciphertext []byte) ([]byte, error) {
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return nil, ErrInvalidKeyFormat
	}

	var priv *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		k, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("rsa: parse PKCS1: %w", err)
		}
		priv = k
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("rsa: parse PKCS8: %w", err)
		}
		rsaKey, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("rsa: PKCS8 key is not RSA")
		}
		priv = rsaKey
	default:
		return nil, ErrInvalidKeyFormat
	}

	return rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
}
