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
	return r5jpGetKey(t, "r5jp:doc", path)
}

func r5jpGetKey(t *testing.T, key, path string) string {
	t.Helper()
	r := testDB.Exec(nil, utils.ToCmdLine("JSON.GET", key, path))
	b, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("JSON.GET %s %s: %s", key, path, r.ToBytes())
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

// ---- R5-20 batch 2 (in/nin) and 3a (arithmetic, size/empty) ----

func TestR5JPInNin(t *testing.T) {
	r5jpSetup(t)
	if got := r5jpGet(t, `$.users[?(@.name in ["alice","carol"])].name`); got != `["alice","carol"]` {
		t.Errorf("in: %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.name nin ["alice","carol"])].name`); got != `["bob"]` {
		t.Errorf("nin: %s", got)
	}
	// Numeric membership with int/float coercion.
	if got := r5jpGet(t, "$.users[?(@.age in [30,45])].name"); got != `["alice","bob"]` {
		t.Errorf("in numbers: %s", got)
	}
	// Missing left side matches neither in nor nin.
	if got := r5jpGet(t, `$.users[?(@.missing in ["x"])]`); got != "[]" {
		t.Errorf("in missing: %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.missing nin ["x"])]`); got != "[]" {
		t.Errorf("nin missing: %s", got)
	}
	// Non-array right side → no match.
	if got := r5jpGet(t, `$.users[?(@.name in "alice")]`); got != "[]" {
		t.Errorf("in non-array: %s", got)
	}
}

func TestR5JPArithmetic(t *testing.T) {
	r5jpSetup(t)
	// @.price + 100 < 2100 style (bikes example).
	if got := r5jpGet(t, "$.users[?(@.age + 10 > 50)].name"); got != `["bob"]` {
		t.Errorf("add: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.age - 5 == 25)].name"); got != `["alice"]` {
		t.Errorf("sub: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.age * 2 == 60)].name"); got != `["alice"]` {
		t.Errorf("mul: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.age / 2 == 15)].name"); got != `["alice"]` {
		t.Errorf("div: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.age % 2 == 0)].name"); got != `["alice"]` {
		t.Errorf("mod: %s", got)
	}
	// Unary minus and precedence: 2*3+4 == 10.
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:n", "$", `[10,16,22]`))
	r := testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:n", "$[?(@ == 2*3+4)]"))
	b, _ := r.(*protocol.BulkReply)
	if b == nil || string(b.Arg) != "[10]" {
		t.Errorf("precedence: %v", r.ToBytes())
	}
	// Division by zero → Nothing → no match, no error.
	if got := r5jpGet(t, "$.users[?(@.age / 0 == 1)].name"); got != "[]" {
		t.Errorf("div zero: %s", got)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:n"))
}

func TestR5JPSizeEmpty(t *testing.T) {
	r5jpSetup(t)
	// tags is an array: alice 2, bob 1, carol 2.
	if got := r5jpGet(t, "$.users[?(@.tags size 2)].name"); got != `["alice","carol"]` {
		t.Errorf("size ==: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.tags size > 1)].name"); got != `["alice","carol"]` {
		t.Errorf("size >: %s", got)
	}
	// String length.
	if got := r5jpGet(t, `$.users[?(@.name size 3)].name`); got != `["bob"]` {
		t.Errorf("name size 3: %s", got)
	}
	// Object member count.
	if got := r5jpGet(t, "$.users[?(@ size 4)].name"); got != `["alice","bob","carol"]` {
		t.Errorf("object size 4: %s", got)
	}
	// empty over arrays and strings.
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:e", "$", `[{"t":[],"s":"x"},{"t":[1],"s":""}]`))
	r := testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:e", "$[?(@.t empty)].s"))
	b, _ := r.(*protocol.BulkReply)
	if b == nil || string(b.Arg) != `["x"]` {
		t.Errorf("empty array: %v", r.ToBytes())
	}
	r = testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:e", "$[?(@.s empty)].t"))
	b, _ = r.(*protocol.BulkReply)
	if b == nil || string(b.Arg) != "[[1]]" {
		t.Errorf("empty string: %v", r.ToBytes())
	}
	// missing field: size/empty are false.
	if got := r5jpGet(t, "$.users[?(@.missing size 0)].name"); got != "[]" {
		t.Errorf("missing size: %s", got)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:e"))
}

// ---- R5-20 batch 3: filter functions and top-level projections ----

func TestR5JPFilterFunctions(t *testing.T) {
	r5jpSetup(t)
	// length() on array/object/string.
	if got := r5jpGet(t, "$.users[?(@.tags.length() == 2)].name"); got != `["alice","carol"]` {
		t.Errorf("length array: %s", got)
	}
	if got := r5jpGet(t, "$.users[?(@.name.length() == 3)].name"); got != `["bob"]` {
		t.Errorf("length string: %s", got)
	}
	// first/last postfix word forms (doc examples: @.colors first == "black").
	if got := r5jpGet(t, `$.users[?(@.tags first == "a")].name`); got != `["alice","bob"]` {
		t.Errorf("first word: %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.tags last == "c")].name`); got != `["carol"]` {
		t.Errorf("last word: %s", got)
	}
	// Aggregations over an array field (r5jp:s root is an array).
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:s", "$", `[{"nums":[1,2,3]},{"nums":[10,20]}]`))
	if got := r5jpGetKey(t, "r5jp:s", "$[?(@.nums.sum() > 10)].nums"); got != "[[10,20]]" {
		t.Errorf("sum: %s", got)
	}
	if got := r5jpGetKey(t, "r5jp:s", "$[?(@.nums.avg() == 2)].nums"); got != "[[1,2,3]]" {
		t.Errorf("avg: %s", got)
	}
	if got := r5jpGetKey(t, "r5jp:s", "$[?(@.nums.min() == 1)].nums"); got != "[[1,2,3]]" {
		t.Errorf("min: %s", got)
	}
	if got := r5jpGetKey(t, "r5jp:s", "$[?(@.nums.max() == 20)].nums"); got != "[[10,20]]" {
		t.Errorf("max: %s", got)
	}
	// match (anchored whole-string) vs search (partial) on strings.
	if got := r5jpGet(t, `$.users[?(@.name.match("alice"))].name`); got != `["alice"]` {
		t.Errorf("match whole: %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.name.match("^a.*e$"))].name`); got != `["alice"]` {
		t.Errorf("match anchors: %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.name.match("a"))].name`); got != "[]" {
		t.Errorf("match partial no: %s", got)
	}
	if got := r5jpGet(t, `$.users[?(@.name.search("o"))].name`); got != `["bob","carol"]` {
		t.Errorf("search partial: %s", got)
	}
	// concat in a comparison (word form with argument).
	if got := r5jpGet(t, `$.users[?(@.name concat "!" == "bob!")].age`); got != "[45]" {
		t.Errorf("concat: %s", got)
	}
	// Numeric functions (r5jp:abs root is an array).
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:abs", "$", `[{"v":-4.2},{"v":3}]`))
	if got := r5jpGetKey(t, "r5jp:abs", "$[?(@.v.abs() > 4)].v"); got != "[-4.2]" {
		t.Errorf("abs: %s", got)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:s", "r5jp:abs"))
}

func TestR5JPTopLevelProjection(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:nums"))
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:nums", "$", `[3,1,2]`))
	r := testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:nums", "min($)"))
	b, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("min($): %s", r.ToBytes())
	}
	if string(b.Arg) != "1" {
		t.Errorf("min($) = %s, want 1", b.Arg)
	}
	r = testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:nums", "sum($)"))
	if b, ok = r.(*protocol.BulkReply); !ok || string(b.Arg) != "6" {
		t.Errorf("sum($): %v", r.ToBytes())
	}
	// length over an absolute path; unknown function → syntax error.
	r = testDB.Exec(nil, utils.ToCmdLine("JSON.GET", "r5jp:nums", "length($)"))
	if b, ok = r.(*protocol.BulkReply); !ok || string(b.Arg) != "3" {
		t.Errorf("length($): %v", r.ToBytes())
	}
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"JSON.GET", "r5jp:nums", "nosuchfn($)")), "ERR Path syntax error")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:nums"))
}

// ---- R5-20 tail: subsetof/anyof/noneof, index(), append() ----

func TestR5JPSetRelationsAndArrayFns(t *testing.T) {
	// Doc-style: colors subsets.
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:c"))
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:c", "$", `[
		{"id":1,"colors":["black","silver"]},
		{"id":2,"colors":["red","gold"]},
		{"id":3,"colors":["black"]}
	]`))
	// subsetof: every element within the given set.
	if got := r5jpGetKey(t, "r5jp:c", "$[?(@.colors subsetof [\"black\",\"silver\",\"white\"])].id"); got != "[1,3]" {
		t.Errorf("subsetof: %s", got)
	}
	// anyof: at least one shared element.
	if got := r5jpGetKey(t, "r5jp:c", `$[?(@.colors anyof ["red","white"])].id`); got != "[2]" {
		t.Errorf("anyof: %s", got)
	}
	// noneof: no shared elements.
	if got := r5jpGetKey(t, "r5jp:c", `$[?(@.colors noneof ["white","gold"])].id`); got != "[1,3]" {
		t.Errorf("noneof: %s", got)
	}
	// index(n) array access; out-of-range → Nothing → no match.
	if got := r5jpGetKey(t, "r5jp:c", `$[?(@.colors.index(0) == "black")].id`); got != "[1,3]" {
		t.Errorf("index(0): %s", got)
	}
	if got := r5jpGetKey(t, "r5jp:c", `$[?(@.colors.index(5) == "x")].id`); got != "[]" {
		t.Errorf("index oob: %s", got)
	}
	// append(): query-time projection adds a value after the elements.
	if got := r5jpGetKey(t, "r5jp:c", `$[?(@.colors.append("teal") anyof ["teal"])].id`); got != "[1,2,3]" {
		t.Errorf("append in filter: %s", got)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:c"))
}

func TestR5JPAppendProjection(t *testing.T) {
	// append() in a filter is read-only (doc untouched); array root.
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:ap"))
	testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "r5jp:ap", "$", `[{"colors":["black"]}]`))
	// Stored document unchanged after append-in-filter (nodelist of one
	// match whose value is the colors array → [["black"]]).
	if got := r5jpGetKey(t, "r5jp:ap", `$[?(@.colors.append("teal") anyof ["teal"])].colors`); got != `[["black"]]` {
		t.Errorf("append read-only: %s", got)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5jp:ap"))
}

// ---- R5-20 tail: wildcards, recursive descent, nodelist semantics ----

func TestR5JPWildcardAndRecursive(t *testing.T) {
	key := "r5jp:wild"
	testDB.Exec(nil, utils.ToCmdLine("DEL", key))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"JSON.SET", key, "$",
		`{"items":[
			{"name":"a","price":10,"tags":["x","y"]},
			{"name":"b","price":20,"tags":["z"]},
			{"name":"c","tags":[]}
		],"nested":{"price":30}}`)), "OK")

	if got := r5jpGetKey(t, key, "$.items[*].price"); got != "[10,20]" {
		t.Errorf("[*] projection: %s", got)
	}
	if got := r5jpGetKey(t, key, "$..price"); got != "[10,20,30]" {
		t.Errorf("recursive descent: %s", got)
	}
	if got := r5jpGetKey(t, key, "$.items[*].tags[*]"); got != `["x","y","z"]` {
		t.Errorf("nested wildcard: %s", got)
	}
	if got := r5jpGetKey(t, key, "sum($.items[*].price)"); got != "30" {
		t.Errorf("sum over nodelist: %s", got)
	}
	if got := r5jpGetKey(t, key, "length($.items[*].price)"); got != "2" {
		t.Errorf("length over nodelist: %s", got)
	}
	if got := r5jpGetKey(t, key, "$.items[*].nope"); got != "[]" {
		t.Errorf("empty nodelist: %s", got)
	}
	// Nodelist comparison: any node satisfies.
	if got := r5jpGetKey(t, key, `$.items[?(@.tags[*] == "z")].name`); got != `["b"]` {
		t.Errorf("nodelist any-match: %s", got)
	}
	// Recursive descent inside a filter relative path.
	if got := r5jpGetKey(t, key, `$.items[?(@..price >= 20)].name`); got != `["b"]` {
		t.Errorf("recursive filter: %s", got)
	}
	// Object values are visited in sorted key order.
	if got := r5jpGetKey(t, key, "$.nested.*"); got != "[30]" {
		t.Errorf("object wildcard: %s", got)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", key))
}
