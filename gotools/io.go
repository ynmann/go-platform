package gotools

import (
	"context"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"
)

// CountingReader wraps an io.Reader and tallies the total bytes read so far.
// Concurrent Reads are not safe; use it on a single read path.
type CountingReader struct {
	R io.Reader
	n int64
}

// Read implements io.Reader and updates the running byte count.
func (cr *CountingReader) Read(p []byte) (int, error) {
	n, err := cr.R.Read(p)
	cr.n += int64(n)
	return n, err
}

// Size returns the cumulative number of bytes successfully read so far.
func (cr *CountingReader) Size() int64 { return cr.n }

// ProgressReader reports streaming progress at most every LogEvery bytes via
// zerolog. Useful for long uploads/downloads where you want a heartbeat
// without per-chunk log noise.
type ProgressReader struct {
	Reader   io.Reader
	Total    uint64
	LogEvery uint64
	Tag      string
}

// Read implements io.Reader and emits an info-level "streamed N bytes" log
// every LogEvery bytes.
func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	if n > 0 {
		pr.Total += uint64(n)
		if pr.LogEvery > 0 && pr.Total%pr.LogEvery < uint64(n) {
			log.Info().Str("tag", pr.Tag).Uint64("bytes", pr.Total).Msg("[progress] streamed")
		}
	}
	return n, err
}

// DetectMime sniffs the first 512 bytes of r to guess its MIME type, then
// rewinds r to the start. r must be seekable; on success the read offset is
// reset to zero.
func DetectMime(r io.ReadSeeker) (string, error) {
	buf := make([]byte, 512)
	n, err := r.Read(buf)
	if err != nil && err != io.EOF {
		return "", err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return http.DetectContentType(buf[:n]), nil
}

// CopyToPipes streams r into every writer in turn, stopping on the first
// write error or context cancellation. It does not close any of the writers,
// which is what makes it safe with io.PipeWriter.
func CopyToPipes(ctx context.Context, r io.Reader, writers ...io.Writer) error {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := r.Read(buf)
		if n > 0 {
			for _, w := range writers {
				if _, werr := w.Write(buf[:n]); werr != nil {
					return werr
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
