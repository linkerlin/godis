package database

// R5-20 JSONPath 批次 1（8.10）：[?(…)] 过滤——任意字面量比较 + `!` 否定
// + && / ||；过滤段必须是最后一段；JSON.SET FPHA 明确拒绝。

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func r5jpSetup(t *testing.T) {
	t.Helper()
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:doc"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "r5jp:doc", "$",
		`{"users":[
			{"name":"alice","age":30,"tags":["a","b"],"active":true},
			{"name":"bob","age":45,"tags":["a"],"active":false},
			{"name":"carol","age":25,"tags":["b","c"],"active":true}
		]}`)), "OK")
}

func r5jpGet(t *testing.T, path string) string {
	t.Helper()
	r := testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:doc", path))
	b, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("JSON.GET %s: %s", path, r.ToBytes())
	}
	return string(b.Arg)
}

func TestR5JPNumericCompare(t *testing.T) {
	r5jpSetup(t)
	// > with relative path on the element.
	if got := r5jpGet(t, "$.users[?(@.age>29)].name"); got != `["alice","bob"]` {
		t.Errorf("age>29: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.age>=45)].name"); got != `["bob"]` {
		t.Errorf("age>=45: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.age<30)].name"); got != `["carol"]` {
		t.Errorf("age<30: %s", got)
	}
	// @ itself compares the element (array of scalars).
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:nums", "$", `[1,5,10]`))
	r := testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:nums", "$[?(@>4)]"))
	b, _ := r.(*protocol.BulkReply)
	if b == nil || string(b.Arg) != "[5,10]" {
		t.Errorf("@>4: %v", r.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:nums"))
}

func TestR5JPEqualityLiterals(t *testing.T) {
	r5jpSetup(t)
	if got := r5jpGet(t, `$.users[?(@.name=="bob")].age`); got != "[45]" {
		t.Errorf("name==bob: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.active==true)].name"); got != `["alice","carol"]` {
		t.Errorf("active==true: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.active==false)].name"); got != `["bob"]` {
		t.Errorf("active==false: %s", got)
	}
	// Array literal compared by deep equality.
	if got := r5jpGet(t, `$.users[?(@.tags==["a","b"])].name`); got != `["alice"]` {
		t.Errorf("tags==[a,b]: %s", got)
	}
	// Object literal.
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:o", "$", `[{"v":{"x":1}},{"v":{"x":2}}]`))
	r := testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:o", `$[?(@.v=={"x":2})].v.x`))
	b, _ := r.(*protocol.BulkReply)
	if b == nil || string(b.Arg) != "[2]" {
		t.Errorf("v=={x:2}: %v", r.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:o"))
	// null literal: missing field == null.
	if got := r5jpGet(t, "$.users[?(@.missing==null)].name"); got != `["alice","bob","carol"]` {
		t.Errorf("missing==null: %s", got)
	}
}

func TestR5JPNegationAndLogic(t *testing.T) {
	r5jpSetup(t)
	if got := r5jpGet(t, "$.users[?(!(@.active))].name"); got != `["bob"]` {
		t.Errorf("!(active): %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.name!="bob")].name`); got != `["alice","carol"]` {
		t.Errorf("name!=bob: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.age>20 && @.age<40)].name"); got != `["alice","carol"]` {
		t.Errorf("20<age<40: %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.name=="alice" || @.name=="carol")].name`); got != `["alice","carol"]` {
		t.Errorf("or: %s", got)
	}
	// ! applied to a bare operand (truthiness).
	if got := r5jpGet(t, "$.users[?(!@.active)].name"); got != `["bob"]` {
		t.Errorf("!active: %s", got)
	}
}

func TestR5JPSyntaxAndSubsetErrors(t *testing.T) {
	r5jpSetup(t)
	// Malformed predicate → error (not the legacy null).
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"JSON.GET", "r5jp:doc", "$.users[?(@.age>)]")),
		"ERR Path syntax error")
	// Filter mid-path: remaining components project over each match.
	if got := r5jpGet(t, "$.users[?(@.age>1)].name"); got != `["alice","bob","carol"]` {
		t.Errorf("projection after filter: %s", got)
	}
	// Filter over a non-array → error.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"JSON.GET", "r5jp:doc", "$[?(@.x>1)]")),
		"ERR Path syntax error")
	// No matches → empty array.
	if got := r5jpGet(t, "$.users[?(@.age>99)].name"); got != "[]" {
		t.Errorf("no matches: %s", got)
	}
}

func TestR5JPFPHARejected(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:f"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "r5jp:f", "$", "[1.5,2.5]", "FPHA")),
		"ERR FPHA is not supported: Godis JSON has no homogeneous float array encoding")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:f", "r5jp:doc"))
}
