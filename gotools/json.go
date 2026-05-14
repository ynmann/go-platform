package gotools

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode"
)

// NormalizeStruct decodes raw into T. If raw is already a T it is returned
// directly; if it is a map[string]any it is round-tripped through JSON. Other
// types yield an error. Useful at boundaries where data arrives as untyped
// JSON (config payloads, generic NATS messages, dynamic webhooks).
func NormalizeStruct[T any](raw any) (T, error) {
	var out T

	switch v := raw.(type) {
	case T:
		return v, nil
	case map[string]any:
		data, err := json.Marshal(v)
		if err != nil {
			return out, fmt.Errorf("json: marshal map: %w", err)
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return out, fmt.Errorf("json: unmarshal into %T: %w", out, err)
		}
		return out, nil
	default:
		return out, fmt.Errorf("json: unexpected content type %T", raw)
	}
}

// SafeUnmarshalJSON tries hard to recover a usable JSON object from a noisy
// byte slice. After a strict json.Unmarshal it falls back to:
//   - stripping non-printable control characters (keeping \n \r \t),
//   - extracting the first balanced {...} object,
//   - dropping object members whose key contains '-',
//   - dropping members whose value starts with an unquoted '-' followed by
//     non-numeric, non-literal characters (e.g. `"key": -bar`).
//
// Intended for ingesting third-party payloads that occasionally arrive
// malformed, not for security-sensitive parsing.
func SafeUnmarshalJSON(body []byte, v any) error {
	if err := json.Unmarshal(body, v); err == nil {
		return nil
	}

	cleaned := bytes.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return -1
		}
		return r
	}, body)

	start, end := -1, -1
	braces := 0
	inQuotes := false
	escaped := false
	for i := 0; i < len(cleaned); i++ {
		c := cleaned[i]
		if c == '\\' && inQuotes {
			escaped = !escaped
			continue
		}
		if c == '"' && !escaped {
			inQuotes = !inQuotes
		}
		escaped = false
		if inQuotes {
			continue
		}
		switch c {
		case '{':
			if start == -1 {
				start = i
			}
			braces++
		case '}':
			braces--
			if braces == 0 && start != -1 {
				end = i + 1
			}
		}
		if end != -1 {
			break
		}
	}

	if start == -1 || end == -1 {
		return json.Unmarshal(cleaned, v)
	}

	processed := scrubJSONFragment(cleaned[start:end])
	return json.Unmarshal(processed, v)
}

// scrubJSONFragment drops object members whose key contains a dash and
// members whose value begins with an unquoted invalid '-' literal. It scans
// linearly without building a parse tree, so it preserves whitespace and
// comma layout where it can.
func scrubJSONFragment(input []byte) []byte {
	var out bytes.Buffer
	inQuotes := false
	inKey := false
	keyHasDash := false
	skipValue := false
	braceLevel := 0

	isDigit := func(b byte) bool { return b >= '0' && b <= '9' }

	for i := 0; i < len(input); i++ {
		c := input[i]

		if c == '"' && (i == 0 || input[i-1] != '\\') {
			inQuotes = !inQuotes
			if inQuotes && !skipValue {
				inKey = true
				keyHasDash = false
			} else {
				inKey = false
			}
			out.WriteByte(c)
			continue
		}

		if inKey && c == '-' {
			keyHasDash = true
		}

		if !inQuotes && c == ':' {
			if keyHasDash && !skipValue {
				skipValue = true
				braceLevel = 0
				continue
			}
			inKey = false
		}

		if skipValue {
			switch {
			case c == '{' || c == '[':
				braceLevel++
			case (c == '}' || c == ']') && braceLevel > 0:
				braceLevel--
			case braceLevel == 0 && (c == ',' || c == '}'):
				skipValue = false
				if c == '}' {
					out.WriteByte('}')
				}
			}
			continue
		}

		if !inQuotes && c == '-' && i+1 < len(input) {
			next := input[i+1]
			if !isDigit(next) && next != '"' && next != '{' && next != '[' && next != 't' && next != 'f' && next != 'n' {
				skipValue = true
				braceLevel = 0
				continue
			}
		}

		out.WriteByte(c)
	}
	return out.Bytes()
}
