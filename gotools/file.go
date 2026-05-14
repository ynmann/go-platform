package gotools

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
)

// FileSHA256 returns the hex-encoded SHA-256 hash of the file at path.
// Streams the file rather than loading it into memory.
func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// CopyFile copies src to dst, overwriting dst if it exists, then fsyncs the
// destination so the data is durable when the function returns.
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

// GetFileExtension returns the lower-case file extension without the leading
// dot. It first tries the filename's extension, then falls back to the first
// extension registered for mimeType. Returns "bin" if neither yields a match.
func GetFileExtension(filename, mimeType string) string {
	if ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filename), ".")); ext != "" {
		return ext
	}
	if mimeType != "" {
		if exts, err := mime.ExtensionsByType(mimeType); err == nil && len(exts) > 0 {
			return strings.TrimPrefix(exts[0], ".")
		}
	}
	return "bin"
}
