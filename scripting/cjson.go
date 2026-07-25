package scripting

import (
	"encoding/json"

	lua "github.com/yuin/gopher-lua"
)

// registerCJSON registers a minimal Redis-compatible cjson module (encode/decode).
func registerCJSON(L *lua.LState) {
	mod := L.NewTable()
	L.SetField(mod, "encode", L.NewFunction(luaCJSONEncode))
	L.SetField(mod, "decode", L.NewFunction(luaCJSONDecode))
	L.SetGlobal("cjson", mod)
}

func luaCJSONEncode(L *lua.LState) int {
	v := L.CheckAny(1)
	goVal := luaToGoForJSON(v)
	b, err := json.Marshal(goVal)
	if err != nil {
		L.RaiseError("cjson.encode: %v", err)
		return 0
	}
	L.Push(lua.LString(string(b)))
	return 1
}

func luaCJSONDecode(L *lua.LState) int {
	s := L.CheckString(1)
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		L.RaiseError("cjson.decode: %v", err)
		return 0
	}
	L.Push(goToLuaForJSON(L, v))
	return 1
}

func luaToGoForJSON(v lua.LValue) interface{} {
	switch val := v.(type) {
	case *lua.LNilType:
		return nil
	case lua.LBool:
		return bool(val)
	case lua.LNumber:
		return float64(val)
	case lua.LString:
		return string(val)
	case *lua.LTable:
		// Distinguish array vs object by consecutive integer keys from 1.
		maxN := 0
		isArray := true
		val.ForEach(func(k, _ lua.LValue) {
			n, ok := k.(lua.LNumber)
			if !ok || float64(int(n)) != float64(n) || n < 1 {
				isArray = false
				return
			}
			if int(n) > maxN {
				maxN = int(n)
			}
		})
		if isArray && maxN > 0 {
			arr := make([]interface{}, maxN)
			for i := 1; i <= maxN; i++ {
				arr[i-1] = luaToGoForJSON(val.RawGetInt(i))
			}
			return arr
		}
		obj := make(map[string]interface{})
		val.ForEach(func(k, vv lua.LValue) {
			obj[k.String()] = luaToGoForJSON(vv)
		})
		return obj
	default:
		return v.String()
	}
}

func goToLuaForJSON(L *lua.LState, v interface{}) lua.LValue {
	switch val := v.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(val)
	case float64:
		return lua.LNumber(val)
	case string:
		return lua.LString(val)
	case []interface{}:
		t := L.NewTable()
		for i, elem := range val {
			t.RawSetInt(i+1, goToLuaForJSON(L, elem))
		}
		return t
	case map[string]interface{}:
		t := L.NewTable()
		for k, elem := range val {
			t.RawSetString(k, goToLuaForJSON(L, elem))
		}
		return t
	default:
		return lua.LString("")
	}
}
