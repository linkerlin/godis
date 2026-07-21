package wildcard

import "errors"

// Pattern represents a Redis-style glob pattern (* ? [...] and \ escapes).
// Matching is done without translating to regular expressions.
type Pattern struct {
	raw string
}

var errEndWithEscape = "end with escape \\"

// CompilePattern validates and wraps a glob pattern.
func CompilePattern(src string) (*Pattern, error) {
	for i := 0; i < len(src); i++ {
		if src[i] == '\\' {
			if i == len(src)-1 {
				return nil, errors.New(errEndWithEscape)
			}
			i++
		}
	}
	return &Pattern{raw: src}, nil
}

// IsMatch returns whether s matches the glob pattern.
func (p *Pattern) IsMatch(s string) bool {
	return matchGlob(p.raw, s)
}

func matchGlob(pattern, s string) bool {
	return matchGlobAt(pattern, 0, s, 0)
}

func matchGlobAt(pattern string, pi int, s string, si int) bool {
	for pi < len(pattern) {
		ch := pattern[pi]
		switch ch {
		case '\\':
			if pi+1 >= len(pattern) {
				return false
			}
			if si >= len(s) || s[si] != pattern[pi+1] {
				return false
			}
			pi += 2
			si++
		case '*':
			// collapse consecutive *
			for pi < len(pattern) && pattern[pi] == '*' {
				pi++
			}
			if pi >= len(pattern) {
				return true
			}
			for ; si <= len(s); si++ {
				if matchGlobAt(pattern, pi, s, si) {
					return true
				}
			}
			return false
		case '?':
			if si >= len(s) {
				return false
			}
			pi++
			si++
		case '[':
			if si >= len(s) {
				return false
			}
			next, ok := matchClass(pattern, pi, s[si])
			if !ok {
				return false
			}
			pi = next
			si++
		default:
			if si >= len(s) || s[si] != ch {
				return false
			}
			pi++
			si++
		}
	}
	return si == len(s)
}

// matchClass parses pattern[pi]=='[' ... ']' against byte b.
// Returns index after ']' and whether matched.
func matchClass(pattern string, pi int, b byte) (int, bool) {
	if pi >= len(pattern) || pattern[pi] != '[' {
		return pi, false
	}
	i := pi + 1
	if i >= len(pattern) {
		// unclosed — treat '[' as literal
		return pi + 1, b == '['
	}
	negate := false
	if pattern[i] == '^' {
		negate = true
		i++
	}
	matched := false
	if i < len(pattern) && pattern[i] == ']' {
		// first char may be literal ]
		matched = b == ']'
		i++
	}
	for i < len(pattern) {
		if pattern[i] == '\\' && i+1 < len(pattern) {
			if b == pattern[i+1] {
				matched = true
			}
			i += 2
			continue
		}
		if pattern[i] == ']' {
			i++
			if negate {
				return i, !matched
			}
			return i, matched
		}
		if i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']' {
			lo, hi := pattern[i], pattern[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if b >= lo && b <= hi {
				matched = true
			}
			i += 3
			continue
		}
		if b == pattern[i] {
			matched = true
		}
		i++
	}
	// unclosed class: fail match (Redis-like)
	return len(pattern), false
}
