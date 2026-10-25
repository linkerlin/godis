package json

import (
	"fmt"
)

// Real RedisJSON (Rust serde_json) parse-error texts, reproduced so client
// tooling sees byte-identical messages. Categories probed against 8.10:
//
//	{bad        → key must be a string at line 1 column 2
//	tru         → EOF while parsing a value at line 1 column 3
//	"unclosed   → EOF while parsing a string at line 1 column 9
//	01          → invalid number at line 1 column 2
//	txt (ident) → expected ident at line 1 column 2
//	y (atom)    → expected value at line 1 column 1
type serdeError struct{ msg string }

func (e *serdeError) Error() string { return e.msg }

// ValidateSerdeJSON checks that data is a well-formed JSON value, returning
// the serde_json-style error when it is not (nil when valid).
func ValidateSerdeJSON(data []byte) error {
	d := &serdeScanner{buf: data}
	d.skipWS()
	if d.pos >= len(d.buf) {
		return &serdeError{"EOF while parsing a value at line 1 column 1"}
	}
	if err := d.value(); err != nil {
		return err
	}
	d.skipWS()
	if d.pos < len(d.buf) {
		return &serdeError{fmt.Sprintf("trailing characters at line 1 column %d", d.pos+1)}
	}
	return nil
}

type serdeScanner struct {
	buf []byte
	pos int
}

func (d *serdeScanner) lineCol() (int, int) { return 1, d.pos + 1 }

func (d *serdeScanner) errf(kind string) error {
	_, col := d.lineCol()
	return &serdeError{fmt.Sprintf("%s at line 1 column %d", kind, col)}
}

func (d *serdeScanner) skipWS() {
	for d.pos < len(d.buf) {
		switch d.buf[d.pos] {
		case ' ', '\t', '\r', '\n':
			d.pos++
		default:
			return
		}
	}
}

func (d *serdeScanner) value() error {
	if d.pos >= len(d.buf) {
		return &serdeError{"EOF while parsing a value at line 1 column " + fmt.Sprint(d.pos + 1)}
	}
	switch c := d.buf[d.pos]; {
	case c == '{':
		d.pos++
		return d.object()
	case c == '[':
		d.pos++
		return d.array()
	case c == '"':
		return d.str()
	case c == 't':
		return d.ident("true")
	case c == 'f':
		return d.ident("false")
	case c == 'n':
		return d.ident("null")
	case c == '-' || (c >= '0' && c <= '9'):
		return d.number()
	default:
		return &serdeError{"expected value at line 1 column " + fmt.Sprint(d.pos + 1)}
	}
}

func (d *serdeScanner) object() error {
	d.skipWS()
	if d.pos >= len(d.buf) {
		return &serdeError{"EOF while parsing an object at line 1 column " + fmt.Sprint(d.pos)}
	}
	if d.buf[d.pos] == '}' {
		d.pos++
		return nil
	}
	for {
		d.skipWS()
		if d.pos >= len(d.buf) {
			return &serdeError{"EOF while parsing an object at line 1 column " + fmt.Sprint(d.pos)}
		}
		if d.buf[d.pos] != '"' {
			return &serdeError{"key must be a string at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
		if err := d.str(); err != nil {
			return err
		}
		d.skipWS()
		if d.pos >= len(d.buf) {
			return &serdeError{"EOF while parsing an object at line 1 column " + fmt.Sprint(d.pos)}
		}
		if d.buf[d.pos] != ':' {
			return &serdeError{"expected `:` at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
		d.pos++
		d.skipWS()
		if err := d.value(); err != nil {
			return err
		}
		d.skipWS()
		if d.pos >= len(d.buf) {
			return &serdeError{"EOF while parsing an object at line 1 column " + fmt.Sprint(d.pos)}
		}
		switch d.buf[d.pos] {
		case ',':
			d.pos++
		case '}':
			d.pos++
			return nil
		default:
			return &serdeError{"expected `,` or `}` at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
	}
}

func (d *serdeScanner) array() error {
	d.skipWS()
	if d.pos >= len(d.buf) {
		return &serdeError{"EOF while parsing a list at line 1 column " + fmt.Sprint(d.pos)}
	}
	if d.buf[d.pos] == ']' {
		d.pos++
		return nil
	}
	for {
		d.skipWS()
		if err := d.value(); err != nil {
			return err
		}
		d.skipWS()
		if d.pos >= len(d.buf) {
			return &serdeError{"EOF while parsing a list at line 1 column " + fmt.Sprint(d.pos)}
		}
		switch d.buf[d.pos] {
		case ',':
			d.pos++
		case ']':
			d.pos++
			return nil
		default:
			return &serdeError{"expected `,` or `]` at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
	}
}

func (d *serdeScanner) str() error {
	d.pos++ // opening quote
	for d.pos < len(d.buf) {
		c := d.buf[d.pos]
		if c == '\\' {
			d.pos += 2
			continue
		}
		if c == '"' {
			d.pos++
			return nil
		}
		d.pos++
	}
	return &serdeError{"EOF while parsing a string at line 1 column " + fmt.Sprint(d.pos)}
}

func (d *serdeScanner) ident(word string) error {
	for i := 0; i < len(word); i++ {
		if d.pos >= len(d.buf) {
			return &serdeError{"EOF while parsing a value at line 1 column " + fmt.Sprint(d.pos)}
		}
		if d.buf[d.pos] != word[i] {
			d.pos++
			return &serdeError{"expected ident at line 1 column " + fmt.Sprint(d.pos)}
		}
		d.pos++
	}
	return nil
}

func (d *serdeScanner) number() error {
	start := d.pos
	if d.buf[d.pos] == '-' {
		d.pos++
	}
	if d.pos >= len(d.buf) {
		return &serdeError{"EOF while parsing a value at line 1 column " + fmt.Sprint(d.pos + 1)}
	}
	// Leading zero is invalid unless it is the whole integer part.
	if d.buf[d.pos] == '0' {
		d.pos++
		if d.pos < len(d.buf) && d.buf[d.pos] >= '0' && d.buf[d.pos] <= '9' {
			return &serdeError{"invalid number at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
	} else {
		digits := 0
		for d.pos < len(d.buf) && d.buf[d.pos] >= '0' && d.buf[d.pos] <= '9' {
			d.pos++
			digits++
		}
		if digits == 0 {
			// No integer part after the optional '-' (e.g. `-x`).
			return &serdeError{"invalid number at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
	}
	if d.pos < len(d.buf) && d.buf[d.pos] == '.' {
		d.pos++
		digits := 0
		for d.pos < len(d.buf) && d.buf[d.pos] >= '0' && d.buf[d.pos] <= '9' {
			d.pos++
			digits++
		}
		if digits == 0 {
			return &serdeError{"invalid number at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
	}
	if d.pos < len(d.buf) && (d.buf[d.pos] == 'e' || d.buf[d.pos] == 'E') {
		d.pos++
		if d.pos < len(d.buf) && (d.buf[d.pos] == '+' || d.buf[d.pos] == '-') {
			d.pos++
		}
		digits := 0
		for d.pos < len(d.buf) && d.buf[d.pos] >= '0' && d.buf[d.pos] <= '9' {
			d.pos++
			digits++
		}
		if digits == 0 {
			return &serdeError{"invalid number at line 1 column " + fmt.Sprint(d.pos + 1)}
		}
	}
	_ = start
	return nil
}
