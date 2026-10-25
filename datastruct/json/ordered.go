package json

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
)

// OrderedObj is an insertion-ordered JSON object, matching real RedisJSON
// wire behavior (keys keep document order; Go maps randomize it).
type OrderedObj struct {
	keys []string
	m    map[string]interface{}
}

// NewOrderedObj creates an empty ordered object.
func NewOrderedObj() *OrderedObj {
	return &OrderedObj{m: make(map[string]interface{})}
}

// Get returns the value for a key.
func (o *OrderedObj) Get(k string) (interface{}, bool) {
	v, ok := o.m[k]
	return v, ok
}

// Set stores a value, appending the key when new (insertion order).
func (o *OrderedObj) Set(k string, v interface{}) {
	if _, exists := o.m[k]; !exists {
		o.keys = append(o.keys, k)
	}
	o.m[k] = v
}

// Delete removes a key. Real RedisJSON fills the hole with the LAST
// member (swap-delete, probed: a b c d e -del b-> a e c d); we match it.
func (o *OrderedObj) Delete(k string) bool {
	if _, exists := o.m[k]; !exists {
		return false
	}
	delete(o.m, k)
	for i, key := range o.keys {
		if key == k {
			last := len(o.keys) - 1
			o.keys[i] = o.keys[last]
			o.keys = o.keys[:last]
			break
		}
	}
	return true
}

// Keys returns the keys in insertion order.
func (o *OrderedObj) Keys() []string { return o.keys }

// Len returns the number of members.
func (o *OrderedObj) Len() int { return len(o.keys) }

// Values returns member values in insertion order.
func (o *OrderedObj) Values() []interface{} {
	out := make([]interface{}, 0, len(o.keys))
	for _, k := range o.keys {
		out = append(out, o.m[k])
	}
	return out
}

// MarshalJSON renders the object with keys in insertion order.
func (o *OrderedObj) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := marshalOrderedValue(o.m[k])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func marshalOrderedValue(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case *OrderedObj:
		return t.MarshalJSON()
	case int64:
		return []byte(strconv.FormatInt(t, 10)), nil
	case float64:
		return []byte(formatJSONFloat(t)), nil
	case []interface{}:
		var buf bytes.Buffer
		buf.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			b, err := marshalOrderedValue(e)
			if err != nil {
				return nil, err
			}
			buf.Write(b)
		}
		buf.WriteByte(']')
		return buf.Bytes(), nil
	default:
		return json.Marshal(v)
	}
}

// formatJSONFloat reproduces real RedisJSON double rendering: whole values
// keep one decimal ("1.0", "100.0"); others use the shortest form.
func formatJSONFloat(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) {
		return strconv.FormatFloat(f, 'f', 1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// UnmarshalJSON parses into insertion order, keeping integers as int64
// and other numbers as float64 (JSON.TYPE's integer/number split).
func (o *OrderedObj) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return strconv.ErrSyntax
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := kt.(string)
		if !ok {
			return strconv.ErrSyntax
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return err
		}
		v, err := parseOrderedValue(raw)
		if err != nil {
			return err
		}
		o.Set(key, v)
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return err
	}
	return nil
}

func parseOrderedValue(raw json.RawMessage) (interface{}, error) {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, strconv.ErrSyntax
	}
	switch trimmed[0] {
	case '{':
		obj := NewOrderedObj()
		if err := obj.UnmarshalJSON(raw); err != nil {
			return nil, err
		}
		return obj, nil
	case '[':
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if _, err := dec.Token(); err != nil { // opening '['
			return nil, strconv.ErrSyntax
		}
		out := make([]interface{}, 0, 4)
		for dec.More() {
			var elem json.RawMessage
			if err := dec.Decode(&elem); err != nil {
				return nil, strconv.ErrSyntax
			}
			v, err := parseOrderedValue(elem)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		if _, err := dec.Token(); err != nil { // closing ']'
			return nil, strconv.ErrSyntax
		}
		return out, nil
	case 't':
		return true, nil
	case 'f':
		return false, nil
	case 'n':
		return nil, nil
	}
	s := string(trimmed)
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	if b, err := strconv.Unquote(s); err == nil {
		return b, nil
	}
	return nil, strconv.ErrSyntax
}

// IsOrderedObj reports whether v is an ordered JSON object.
func IsOrderedObj(v interface{}) bool {
	_, ok := v.(*OrderedObj)
	return ok
}

// ObjAsOrdered casts v when possible.
func ObjAsOrdered(v interface{}) (*OrderedObj, bool) {
	o, ok := v.(*OrderedObj)
	return o, ok
}

// ConvertToOrdered rewrites a generic decoded value (maps from legacy
// storage / legacy callers) into the ordered representation. Map key order
// is unspecified for legacy values; fresh parses keep document order.
func ConvertToOrdered(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		obj := NewOrderedObj()
		for k, val := range t {
			obj.Set(k, ConvertToOrdered(val))
		}
		return obj
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = ConvertToOrdered(e)
		}
		return out
	case float64:
		if t == math.Trunc(t) {
			return int64(t)
		}
		return t
	default:
		return v
	}
}
