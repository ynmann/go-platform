package gotools

// Chunk splits s into successive sub-slices of size chunkSize. The final
// chunk may be shorter than chunkSize. Returns nil for chunkSize <= 0 or an
// empty input. The returned sub-slices share backing storage with s.
func Chunk[T any](s []T, chunkSize int) [][]T {
	if chunkSize <= 0 || len(s) == 0 {
		return nil
	}
	out := make([][]T, 0, (len(s)+chunkSize-1)/chunkSize)
	for i := 0; i < len(s); i += chunkSize {
		end := i + chunkSize
		if end > len(s) {
			end = len(s)
		}
		out = append(out, s[i:end])
	}
	return out
}

// Unique returns a slice containing each distinct element of s in the order
// of its first appearance. Allocates a single map of size up to len(s).
func Unique[T comparable](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	seen := make(map[T]struct{}, len(s))
	out := make([]T, 0, len(s))
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
