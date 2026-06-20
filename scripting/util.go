package scripting

import (
	"fmt"
	"strconv"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
	lua "github.com/yuin/gopher-lua"
)

// luaValueToGo converts a Lua value to Go interface{}
func luaValueToGo(lv lua.LValue) interface{} {
	switch v := lv.(type) {
	case *lua.LNilType:
		return nil
	case lua.LBool:
		return bool(v)
	case lua.LNumber:
		// Try to return as integer if it's a whole number
		if float64(v) == float64(int64(v)) {
			return int64(v)
		}
		return float64(v)
	case lua.LString:
		return string(v)
	case *lua.LTable:
		return luaTableToGo(v)
	case *lua.LFunction:
		return fmt.Sprintf("function: %p", v)
	case *lua.LUserData:
		return v.Value
	default:
		return fmt.Sprintf("%v", v)
	}
}

// luaTableToGo converts a Lua table to Go map or slice
func luaTableToGo(tbl *lua.LTable) interface{} {
	// Check if it's an array (has consecutive integer keys starting from 1)
	isArray := true
	maxn := tbl.MaxN()

	if maxn == 0 {
		isArray = false
	} else {
		// Check if all keys from 1 to maxn exist
		for i := 1; i <= maxn; i++ {
			if tbl.RawGetInt(i) == lua.LNil {
				isArray = false
				break
			}
		}
	}

	if isArray {
		// Convert to slice
		result := make([]interface{}, 0, maxn)
		tbl.ForEach(func(key, value lua.LValue) {
			if num, ok := key.(lua.LNumber); ok {
				idx := int(num) - 1 // Lua is 1-indexed, Go is 0-indexed
				if idx >= 0 && idx < maxn {
					result = append(result, luaValueToGo(value))
				}
			}
		})
		return result
	}

	// Convert to map
	result := make(map[string]interface{})
	tbl.ForEach(func(key, value lua.LValue) {
		keyStr := luaValueToString(key)
		result[keyStr] = luaValueToGo(value)
	})
	return result
}

// luaValueToArg converts a Lua value to string argument for Redis commands
// Unlike luaValueToString, this does not add quotes for strings
func luaValueToArg(lv lua.LValue) string {
	switch v := lv.(type) {
	case *lua.LNilType:
		return ""
	case lua.LBool:
		if v {
			return "1"
		}
		return "0"
	case lua.LNumber:
		return fmt.Sprintf("%g", float64(v))
	case lua.LString:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// pushGoValue pushes a Go value onto the Lua stack
func pushGoValue(L *lua.LState, v interface{}) {
	lv := goValueToLua(L, v)
	L.Push(lv)
}

// goValueToLua converts a Go value to Lua LValue
func goValueToLua(L *lua.LState, v interface{}) lua.LValue {
	if v == nil {
		return lua.LNil
	}

	switch val := v.(type) {
	case string:
		return lua.LString(val)
	case []byte:
		return lua.LString(val)
	case int:
		return lua.LNumber(val)
	case int8:
		return lua.LNumber(val)
	case int16:
		return lua.LNumber(val)
	case int32:
		return lua.LNumber(val)
	case int64:
		return lua.LNumber(val)
	case uint:
		return lua.LNumber(val)
	case uint8:
		return lua.LNumber(val)
	case uint16:
		return lua.LNumber(val)
	case uint32:
		return lua.LNumber(val)
	case uint64:
		return lua.LNumber(val)
	case float32:
		return lua.LNumber(val)
	case float64:
		return lua.LNumber(val)
	case bool:
		return lua.LBool(val)
	case []interface{}:
		tbl := L.NewTable()
		for i, elem := range val {
			L.SetTable(tbl, lua.LNumber(i+1), goValueToLua(L, elem))
		}
		return tbl
	case map[string]interface{}:
		tbl := L.NewTable()
		for k, v := range val {
			L.SetField(tbl, k, goValueToLua(L, v))
		}
		return tbl
	case error:
		return lua.LString(val.Error())
	default:
		return lua.LString(fmt.Sprintf("%v", val))
	}
}

// ConvertToRedisReply converts a Go value to Redis reply
// This function is kept for backward compatibility
func ConvertToRedisReply(v interface{}) redis.Reply {
	if v == nil {
		return &protocol.NullBulkReply{}
	}

	switch val := v.(type) {
	case string:
		return protocol.MakeBulkReply([]byte(val))
	case []byte:
		return protocol.MakeBulkReply(val)
	case int:
		return protocol.MakeIntReply(int64(val))
	case int64:
		return protocol.MakeIntReply(val)
	case float64:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%g", val)))
	case bool:
		if val {
			return protocol.MakeIntReply(1)
		}
		return protocol.MakeIntReply(0)
	case []interface{}:
		var elems [][]byte
		for _, elem := range val {
			r := ConvertToRedisReply(elem)
			elems = append(elems, r.ToBytes())
		}
		return protocol.MakeMultiBulkReply(elems)
	case map[string]interface{}:
		// Check for error reply (used by pcall)
		if errVal, ok := val["err"]; ok {
			return protocol.MakeErrReply(fmt.Sprintf("-%v", errVal))
		}
		// Check for status reply
		if okVal, ok := val["ok"]; ok {
			return ConvertToRedisReply(okVal)
		}
		var elems [][]byte
		for k, v := range val {
			elems = append(elems, []byte(k))
			r := ConvertToRedisReply(v)
			elems = append(elems, r.ToBytes())
		}
		return protocol.MakeMultiBulkReply(elems)
	case error:
		return protocol.MakeErrReply(val.Error())
	default:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%v", val)))
	}
}

// ConvertLuaResultToRedisReply converts a Lua execution result to Redis reply
func ConvertLuaResultToRedisReply(result interface{}) redis.Reply {
	return ConvertToRedisReply(result)
}

// ParseLuaNumber parses a Lua number from string
func ParseLuaNumber(s string) (lua.LNumber, bool) {
	// Try integer first
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return lua.LNumber(i), true
	}

	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return lua.LNumber(f), true
	}

	return 0, false
}

// IsTruthy checks if a Lua value is truthy
func IsTruthy(lv lua.LValue) bool {
	if lv == lua.LNil {
		return false
	}

	if b, ok := lv.(lua.LBool); ok {
		return bool(b)
	}

	// Everything else is truthy
	return true
}

// TableToStringSlice converts a Lua table to a string slice
func TableToStringSlice(tbl *lua.LTable) []string {
	var result []string
	tbl.ForEach(func(key, value lua.LValue) {
		result = append(result, luaValueToString(value))
	})
	return result
}

// TableToMap converts a Lua table to a map
func TableToMap(tbl *lua.LTable) map[string]string {
	result := make(map[string]string)
	tbl.ForEach(func(key, value lua.LValue) {
		keyStr := luaValueToString(key)
		valStr := luaValueToString(value)
		result[keyStr] = valStr
	})
	return result
}
