package scripting

import (
	"encoding/binary"
	"fmt"
	"math"

	lua "github.com/yuin/gopher-lua"
)

// registerCMsgPack registers a minimal Redis-compatible cmsgpack module (pack/unpack).
func registerCMsgPack(L *lua.LState) {
	mod := L.NewTable()
	L.SetField(mod, "pack", L.NewFunction(luaCMsgPackPack))
	L.SetField(mod, "unpack", L.NewFunction(luaCMsgPackUnpack))
	L.SetGlobal("cmsgpack", mod)
}

func luaCMsgPackPack(L *lua.LState) int {
	n := L.GetTop()
	var out []byte
	for i := 1; i <= n; i++ {
		b, err := msgpackEncode(luaToGoForJSON(L.CheckAny(i)))
		if err != nil {
			L.RaiseError("cmsgpack.pack: %v", err)
			return 0
		}
		out = append(out, b...)
	}
	L.Push(lua.LString(string(out)))
	return 1
}

func luaCMsgPackUnpack(L *lua.LState) int {
	s := L.CheckString(1)
	vals, _, err := msgpackDecodeMany([]byte(s))
	if err != nil {
		L.RaiseError("cmsgpack.unpack: %v", err)
		return 0
	}
	for _, v := range vals {
		L.Push(goToLuaForJSON(L, v))
	}
	return len(vals)
}

func msgpackEncode(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case nil:
		return []byte{0xc0}, nil
	case bool:
		if val {
			return []byte{0xc3}, nil
		}
		return []byte{0xc2}, nil
	case float64:
		if val == float64(int64(val)) && !math.IsInf(val, 0) && !math.IsNaN(val) {
			return msgpackEncodeInt(int64(val)), nil
		}
		buf := make([]byte, 9)
		buf[0] = 0xcb
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(val))
		return buf, nil
	case string:
		b := []byte(val)
		n := len(b)
		var hdr []byte
		switch {
		case n <= 31:
			hdr = []byte{0xa0 | byte(n)}
		case n <= 0xff:
			hdr = []byte{0xd9, byte(n)}
		case n <= 0xffff:
			hdr = []byte{0xda, byte(n >> 8), byte(n)}
		default:
			hdr = []byte{0xdb, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
		}
		return append(hdr, b...), nil
	case []interface{}:
		n := len(val)
		var hdr []byte
		switch {
		case n <= 15:
			hdr = []byte{0x90 | byte(n)}
		case n <= 0xffff:
			hdr = []byte{0xdc, byte(n >> 8), byte(n)}
		default:
			hdr = []byte{0xdd, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
		}
		out := hdr
		for _, elem := range val {
			eb, err := msgpackEncode(elem)
			if err != nil {
				return nil, err
			}
			out = append(out, eb...)
		}
		return out, nil
	case map[string]interface{}:
		n := len(val)
		var hdr []byte
		switch {
		case n <= 15:
			hdr = []byte{0x80 | byte(n)}
		case n <= 0xffff:
			hdr = []byte{0xde, byte(n >> 8), byte(n)}
		default:
			hdr = []byte{0xdf, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
		}
		out := hdr
		for k, vv := range val {
			kb, err := msgpackEncode(k)
			if err != nil {
				return nil, err
			}
			vb, err := msgpackEncode(vv)
			if err != nil {
				return nil, err
			}
			out = append(out, kb...)
			out = append(out, vb...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func msgpackEncodeInt(n int64) []byte {
	if n >= 0 && n <= 127 {
		return []byte{byte(n)}
	}
	if n >= -32 && n < 0 {
		return []byte{byte(int8(n))}
	}
	if n >= math.MinInt8 && n <= math.MaxInt8 {
		return []byte{0xd0, byte(int8(n))}
	}
	if n >= math.MinInt16 && n <= math.MaxInt16 {
		return []byte{0xd1, byte(n >> 8), byte(n)}
	}
	if n >= math.MinInt32 && n <= math.MaxInt32 {
		buf := make([]byte, 5)
		buf[0] = 0xd2
		binary.BigEndian.PutUint32(buf[1:], uint32(int32(n)))
		return buf
	}
	buf := make([]byte, 9)
	buf[0] = 0xd3
	binary.BigEndian.PutUint64(buf[1:], uint64(n))
	return buf
}

func msgpackDecodeMany(b []byte) ([]interface{}, int, error) {
	var vals []interface{}
	off := 0
	for off < len(b) {
		v, n, err := msgpackDecodeOne(b[off:])
		if err != nil {
			return nil, 0, err
		}
		vals = append(vals, v)
		off += n
	}
	return vals, off, nil
}

func msgpackDecodeOne(b []byte) (interface{}, int, error) {
	if len(b) == 0 {
		return nil, 0, fmt.Errorf("truncated")
	}
	code := b[0]
	switch {
	case code <= 0x7f:
		return float64(code), 1, nil
	case code >= 0xe0:
		return float64(int8(code)), 1, nil
	case code == 0xc0:
		return nil, 1, nil
	case code == 0xc2:
		return false, 1, nil
	case code == 0xc3:
		return true, 1, nil
	case code == 0xcc:
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return float64(b[1]), 2, nil
	case code == 0xcd:
		if len(b) < 3 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return float64(binary.BigEndian.Uint16(b[1:])), 3, nil
	case code == 0xce:
		if len(b) < 5 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return float64(binary.BigEndian.Uint32(b[1:])), 5, nil
	case code == 0xd0:
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return float64(int8(b[1])), 2, nil
	case code == 0xd1:
		if len(b) < 3 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return float64(int16(binary.BigEndian.Uint16(b[1:]))), 3, nil
	case code == 0xd2:
		if len(b) < 5 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return float64(int32(binary.BigEndian.Uint32(b[1:]))), 5, nil
	case code == 0xd3:
		if len(b) < 9 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return float64(int64(binary.BigEndian.Uint64(b[1:]))), 9, nil
	case code == 0xcb:
		if len(b) < 9 {
			return nil, 0, fmt.Errorf("truncated")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(b[1:])), 9, nil
	case code >= 0xa0 && code <= 0xbf:
		n := int(code & 0x1f)
		if len(b) < 1+n {
			return nil, 0, fmt.Errorf("truncated")
		}
		return string(b[1 : 1+n]), 1 + n, nil
	case code == 0xd9:
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("truncated")
		}
		n := int(b[1])
		if len(b) < 2+n {
			return nil, 0, fmt.Errorf("truncated")
		}
		return string(b[2 : 2+n]), 2 + n, nil
	case code == 0xda:
		if len(b) < 3 {
			return nil, 0, fmt.Errorf("truncated")
		}
		n := int(binary.BigEndian.Uint16(b[1:]))
		if len(b) < 3+n {
			return nil, 0, fmt.Errorf("truncated")
		}
		return string(b[3 : 3+n]), 3 + n, nil
	case code >= 0x90 && code <= 0x9f:
		n := int(code & 0x0f)
		return msgpackDecodeArray(b, 1, n)
	case code == 0xdc:
		if len(b) < 3 {
			return nil, 0, fmt.Errorf("truncated")
		}
		n := int(binary.BigEndian.Uint16(b[1:]))
		return msgpackDecodeArray(b, 3, n)
	case code >= 0x80 && code <= 0x8f:
		n := int(code & 0x0f)
		return msgpackDecodeMap(b, 1, n)
	case code == 0xde:
		if len(b) < 3 {
			return nil, 0, fmt.Errorf("truncated")
		}
		n := int(binary.BigEndian.Uint16(b[1:]))
		return msgpackDecodeMap(b, 3, n)
	default:
		return nil, 0, fmt.Errorf("unsupported msgpack code 0x%02x", code)
	}
}

func msgpackDecodeArray(b []byte, hdr, n int) (interface{}, int, error) {
	out := make([]interface{}, n)
	off := hdr
	for i := 0; i < n; i++ {
		v, m, err := msgpackDecodeOne(b[off:])
		if err != nil {
			return nil, 0, err
		}
		out[i] = v
		off += m
	}
	return out, off, nil
}

func msgpackDecodeMap(b []byte, hdr, n int) (interface{}, int, error) {
	out := make(map[string]interface{}, n)
	off := hdr
	for i := 0; i < n; i++ {
		k, m, err := msgpackDecodeOne(b[off:])
		if err != nil {
			return nil, 0, err
		}
		off += m
		v, m, err := msgpackDecodeOne(b[off:])
		if err != nil {
			return nil, 0, err
		}
		off += m
		out[fmt.Sprint(k)] = v
	}
	return out, off, nil
}
