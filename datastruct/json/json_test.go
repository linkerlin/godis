package json

import (
	"encoding/json"
	"math"
	"testing"
)

func mustJSON(t *testing.T, s string) *JSONValue {
	t.Helper()
	jv, err := NewJSONValueFromBytes([]byte(s))
	if err != nil {
		t.Fatalf("NewJSONValueFromBytes: %v", err)
	}
	return jv
}

func TestPathHasNodelist(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"$.items[*].price", true},
		{"$..price", true},
		{"$.*", true},
		{"$.items[?(@.tags[*] == \"z\")].name", false},
		{"$.a.b", false},
		{"$.a[0]", false},
	}
	for _, tc := range cases {
		if got := PathHasNodelist(tc.path); got != tc.want {
			t.Errorf("PathHasNodelist(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestGetWildcardAndRecursive(t *testing.T) {
	jv := mustJSON(t, `{"items":[{"price":10},{"price":20},{"name":"c"}],"nested":{"price":30}}`)
	v, err := jv.Get("$.items[*].price")
	if err != nil {
		t.Fatalf("wildcard: %v", err)
	}
	nl, ok := v.(nodeList)
	if !ok || len(nl) != 2 || nl[0] != float64(10) || nl[1] != float64(20) {
		t.Fatalf("wildcard = %#v, want [10 20]", v)
	}
	v, err = jv.Get("$..price")
	if err != nil {
		t.Fatalf("recursive: %v", err)
	}
	nl, ok = v.(nodeList)
	if !ok || len(nl) != 3 || nl[0] != float64(10) || nl[1] != float64(20) || nl[2] != float64(30) {
		t.Fatalf("recursive = %#v, want [10 20 30]", v)
	}
}

func TestProjectionArrayWrapping(t *testing.T) {
	jv := mustJSON(t, `[3,1,2]`)
	v, err := jv.Get("sum($)")
	if err != nil {
		t.Fatalf("sum($): %v", err)
	}
	b, err := json.Marshal(v)
	if err != nil || string(b) != "[6.0]" {
		t.Fatalf("sum($) = %s/%v, want [6.0]", b, err)
	}
	v, err = jv.Get("length($)")
	if err != nil {
		t.Fatalf("length($): %v", err)
	}
	b, _ = json.Marshal(v)
	if string(b) != "[3]" {
		t.Fatalf("length($) = %s, want [3]", b)
	}

	wild := mustJSON(t, `{"items":[{"price":10},{"price":20},{"name":"c"}]}`)
	v, err = wild.Get("sum($.items[*].price)")
	if err != nil {
		t.Fatalf("sum wildcard: %v", err)
	}
	b, _ = json.Marshal(v)
	if string(b) != "[30.0]" {
		t.Fatalf("sum wildcard = %s, want [30.0]", b)
	}
	v, err = wild.Get("length($.items[*].price)")
	if err != nil {
		t.Fatalf("length wildcard: %v", err)
	}
	b, _ = json.Marshal(v)
	if string(b) != "[]" {
		t.Fatalf("length wildcard = %s, want []", b)
	}
}

func TestNodeListFilterAnyMatch(t *testing.T) {
	arr := []interface{}{
		map[string]interface{}{"name": "a", "tags": []interface{}{"x", "y"}},
		map[string]interface{}{"name": "b", "tags": []interface{}{"z"}},
	}
	out, err := ApplyFilter(arr, `?(@.tags[*] == "z")`)
	if err != nil {
		t.Fatalf("ApplyFilter: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("filter matches = %d, want 1", len(out))
	}
	if m, ok := out[0].(map[string]interface{}); !ok || m["name"] != "b" {
		t.Fatalf("filter match = %#v, want b", out[0])
	}
}

func TestProjectionFloatFormatting(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want string
	}{
		{6, "6.0"},
		{6.5, "6.5"},
		{-3, "-3.0"},
		{math.NaN(), "null"},
	} {
		b, err := json.Marshal(projectionFloat(tc.in))
		if err != nil || string(b) != tc.want {
			t.Errorf("marshal projectionFloat(%v) = %s/%v, want %s", tc.in, b, err, tc.want)
		}
	}
}

func TestGetSetDelType(t *testing.T) {
	jv := mustJSON(t, `{"a":{"b":[1,2]},"s":"x"}`)
	if v, err := jv.Get("$.a.b[1]"); err != nil || v != float64(2) {
		t.Fatalf("Get nested = %#v/%v", v, err)
	}
	if typ, err := jv.Type("$.a"); err != nil || typ != "object" {
		t.Fatalf("Type object = %q/%v", typ, err)
	}
	if typ, err := jv.Type("$.a.b"); err != nil || typ != "array" {
		t.Fatalf("Type array = %q/%v", typ, err)
	}
	if typ, err := jv.Type("$.s"); err != nil || typ != "string" {
		t.Fatalf("Type string = %q/%v", typ, err)
	}
	if ok, err := jv.Set("$.a.b[0]", "changed", false, false); err != nil || !ok {
		t.Fatalf("Set = %v/%v", ok, err)
	}
	if v, _ := jv.Get("$.a.b[0]"); v != "changed" {
		t.Fatalf("after Set = %#v", v)
	}
	if ok, err := jv.Del("$.s"); err != nil || !ok {
		t.Fatalf("Del = %v/%v", ok, err)
	}
	if _, err := jv.Get("$.s"); err == nil {
		t.Fatal("deleted key should no longer resolve")
	}
}

func mustGet(t *testing.T, jv *JSONValue, path string) interface{} {
	t.Helper()
	v, err := jv.Get(path)
	if err != nil {
		t.Fatalf("Get(%s): %v", path, err)
	}
	return v
}

func TestApplyFilterOperators(t *testing.T) {
	arr := []interface{}{
		map[string]interface{}{"name": "alice", "age": float64(30), "tags": []interface{}{"a", "b"}},
		map[string]interface{}{"name": "bob", "age": float64(45), "tags": []interface{}{"a"}},
		map[string]interface{}{"name": "carol", "age": float64(25), "tags": []interface{}{"b", "c"}},
	}
	cases := []struct {
		pred string
		want int
	}{
		{`?(@.age > 30)`, 1},
		{`?(@.age >= 30 && @.age < 40)`, 1},
		{`?(@.name in ["alice","carol"])`, 2},
		{`?(@.name nin ["alice"])`, 2},
		{`?(@.tags.length() == 2)`, 2},
		{`?(@.name.match("^(alice|bob)$"))`, 2},
		{`?(@.name.search("o"))`, 2},
		{`?(@.tags first == "a")`, 2},
		{`?(@.age + 10 > 40)`, 1},
	}
	for _, tc := range cases {
		out, err := ApplyFilter(arr, tc.pred)
		if err != nil {
			t.Fatalf("ApplyFilter(%s): %v", tc.pred, err)
		}
		if len(out) != tc.want {
			t.Errorf("ApplyFilter(%s) = %d, want %d", tc.pred, len(out), tc.want)
		}
	}
}

func TestLengthOfAndTruthy(t *testing.T) {
	if n, ok := lengthOf("你好"); !ok || n != 2 {
		t.Fatalf("lengthOf string = %d/%v", n, ok)
	}
	if n, ok := lengthOf(nodeList{1, 2}); !ok || n != 2 {
		t.Fatalf("lengthOf nodeList = %d/%v", n, ok)
	}
	if truthy(nodeList{}) || !truthy(nodeList{1}) {
		t.Fatal("truthy nodeList empty/non-empty mismatch")
	}
}

func TestProjectionUnknownFunctionError(t *testing.T) {
	jv := mustJSON(t, `[1,2]`)
	if _, err := jv.Get("nosuchfn($)"); err == nil {
		t.Fatal("unknown projection function should error")
	}
}

func TestCompareNodeList(t *testing.T) {
	if ok, err := compare(nodeList{float64(1), float64(2)}, float64(2), "=="); err != nil || !ok {
		t.Fatalf("nodelist any-match ==: %v/%v", ok, err)
	}
	if ok, err := compare(nodeList{float64(1), float64(2)}, float64(1), ">"); err != nil || !ok {
		t.Fatalf("nodelist any-match >: %v/%v", ok, err)
	}
	if ok, err := compare([]interface{}{float64(1)}, []interface{}{float64(1)}, "=="); err != nil || !ok {
		t.Fatalf("plain array deep equality: %v/%v", ok, err)
	}
}
