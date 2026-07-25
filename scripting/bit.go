package scripting

import (
	"math"

	lua "github.com/yuin/gopher-lua"
)

// registerBit registers a minimal Redis-compatible bit (LuaBitOp) module.
func registerBit(L *lua.LState) {
	mod := L.NewTable()
	L.SetField(mod, "tobit", L.NewFunction(luaBitTobit))
	L.SetField(mod, "band", L.NewFunction(luaBitBand))
	L.SetField(mod, "bor", L.NewFunction(luaBitBor))
	L.SetField(mod, "bxor", L.NewFunction(luaBitBxor))
	L.SetField(mod, "bnot", L.NewFunction(luaBitBnot))
	L.SetField(mod, "lshift", L.NewFunction(luaBitLshift))
	L.SetField(mod, "rshift", L.NewFunction(luaBitRshift))
	L.SetField(mod, "arshift", L.NewFunction(luaBitArshift))
	L.SetGlobal("bit", mod)
}

func luaBitTobit(L *lua.LState) int {
	L.Push(lua.LNumber(bitTobit(L.CheckNumber(1))))
	return 1
}

func luaBitBand(L *lua.LState) int {
	n := L.GetTop()
	if n < 1 {
		L.RaiseError("bit.band needs at least 1 argument")
		return 0
	}
	r := bitTobit(L.CheckNumber(1))
	for i := 2; i <= n; i++ {
		r &= bitTobit(L.CheckNumber(i))
	}
	L.Push(lua.LNumber(r))
	return 1
}

func luaBitBor(L *lua.LState) int {
	n := L.GetTop()
	if n < 1 {
		L.RaiseError("bit.bor needs at least 1 argument")
		return 0
	}
	r := bitTobit(L.CheckNumber(1))
	for i := 2; i <= n; i++ {
		r |= bitTobit(L.CheckNumber(i))
	}
	L.Push(lua.LNumber(r))
	return 1
}

func luaBitBxor(L *lua.LState) int {
	n := L.GetTop()
	if n < 1 {
		L.RaiseError("bit.bxor needs at least 1 argument")
		return 0
	}
	r := bitTobit(L.CheckNumber(1))
	for i := 2; i <= n; i++ {
		r ^= bitTobit(L.CheckNumber(i))
	}
	L.Push(lua.LNumber(r))
	return 1
}

func luaBitBnot(L *lua.LState) int {
	L.Push(lua.LNumber(^bitTobit(L.CheckNumber(1))))
	return 1
}

func luaBitLshift(L *lua.LState) int {
	x := bitTobit(L.CheckNumber(1))
	n := int(L.CheckNumber(2)) & 31
	L.Push(lua.LNumber(int32(uint32(x) << uint(n))))
	return 1
}

func luaBitRshift(L *lua.LState) int {
	x := bitTobit(L.CheckNumber(1))
	n := int(L.CheckNumber(2)) & 31
	L.Push(lua.LNumber(int32(uint32(x) >> uint(n))))
	return 1
}

func luaBitArshift(L *lua.LState) int {
	x := bitTobit(L.CheckNumber(1))
	n := int(L.CheckNumber(2)) & 31
	L.Push(lua.LNumber(x >> uint(n)))
	return 1
}

// bitTobit coerces to signed 32-bit like LuaBitOp / Redis bit library.
func bitTobit(n lua.LNumber) int32 {
	f := float64(n)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return int32(int64(f))
}