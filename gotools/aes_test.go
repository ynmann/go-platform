package gotools_test

import (
	"bytes"
	"testing"

	"github.com/ynmann/go-platform/gotools"
)

func TestEncryptDecryptAES256GCM(t *testing.T) {
	key, err := gotools.GenerateAES256Key()
	if err != nil {
		t.Fatalf("GenerateAES256Key: %v", err)
	}
	plain := []byte("hello, this is a secret message")

	enc, err := gotools.EncryptAES256GCM(key, plain)
	if err != nil {
		t.Fatalf("EncryptAES256GCM: %v", err)
	}

	dec, err := gotools.DecryptAES256GCM(key, enc)
	if err != nil {
		t.Fatalf("DecryptAES256GCM: %v", err)
	}

	if !bytes.Equal(plain, dec) {
		t.Fatalf("round-trip mismatch: got %q want %q", dec, plain)
	}
}

func TestEncryptDecryptString(t *testing.T) {
	const secret = "correct horse battery staple"
	const plain = "лёгкий русский текст с unicode"

	enc, err := gotools.EncryptString(secret, plain)
	if err != nil {
		t.Fatalf("EncryptString: %v", err)
	}

	dec, err := gotools.DecryptString(secret, enc)
	if err != nil {
		t.Fatalf("DecryptString: %v", err)
	}

	if dec != plain {
		t.Fatalf("round-trip mismatch: got %q want %q", dec, plain)
	}
}

func TestDecryptAES256GCM_ShortCiphertext(t *testing.T) {
	key, _ := gotools.GenerateAES256Key()
	if _, err := gotools.DecryptAES256GCM(key, []byte("short")); err == nil {
		t.Fatalf("expected error on short ciphertext, got nil")
	}
}
