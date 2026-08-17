package redisearch

import (
	"strings"
	"testing"
)

// TestApplyNumericStringCoerce aligns floor/ceil/abs with Redis 8.6 QE:
// numeric strings (leading space OK) and bool coerce; trailing space → nan.
func TestApplyNumericStringCoerce(t *testing.T) {
	got, err := EvalApplyExpr("floor('3.7')", nil)
	if err != nil || got != float64(3) {
		t.Fatalf("floor('3.7'): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("floor(' 3.7')", nil)
	if err != nil || got != float64(3) {
		t.Fatalf("floor(' 3.7'): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("ceil('3.2')", nil)
	if err != nil || got != float64(4) {
		t.Fatalf("ceil('3.2'): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("abs('-5')", nil)
	if err != nil || got != float64(5) {
		t.Fatalf("abs('-5'): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("abs(' -5')", nil)
	if err != nil || got != float64(5) {
		t.Fatalf("abs(' -5'): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("floor('3.7 ')", nil)
	if err != nil || got != "nan" {
		t.Fatalf("floor trailing space want nan: got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("floor(1==1)", nil)
	if err != nil || got != float64(1) {
		t.Fatalf("floor(true): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("floor(1==0)", nil)
	if err != nil || got != float64(0) {
		t.Fatalf("floor(false): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("sqrt('9')", nil)
	if err != nil || got != float64(3) {
		t.Fatalf("sqrt('9'): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("log2('0')", nil)
	if err != nil || got != "-inf" {
		t.Fatalf("log2(0) want -inf: got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("sqrt('-1')", nil)
	if err != nil || got != "-nan" {
		t.Fatalf("sqrt(-1) want -nan: got %v err %v", got, err)
	}
}

// TestApplyStringTypeChecks aligns upper/strlen/substr/startswith with Redis 8.6.
func TestApplyStringTypeChecks(t *testing.T) {
	fields := map[string]interface{}{"n": float64(10), "t": "Hello"}

	got, err := EvalApplyExpr("upper(@n)", fields)
	if err != nil || got != nil {
		t.Fatalf("upper(@n) want Null: got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("lower(1==1)", nil)
	if err != nil || got != nil {
		t.Fatalf("lower(bool) want Null: got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("upper(@t)", fields)
	if err != nil || got != "HELLO" {
		t.Fatalf("upper(@t): got %v err %v", got, err)
	}

	_, err = EvalApplyExpr("strlen(@n)", fields)
	if err == nil || !strings.Contains(err.Error(), "Invalid type (1) for argument 0 in function 'strlen'") {
		t.Fatalf("strlen(@n): %v", err)
	}
	_, err = EvalApplyExpr("startswith(@n,'1')", fields)
	if err == nil || !strings.Contains(err.Error(), "startswith") {
		t.Fatalf("startswith(@n): %v", err)
	}
	_, err = EvalApplyExpr("startswith(@t,@n)", fields)
	if err == nil || !strings.Contains(err.Error(), "argument 1") {
		t.Fatalf("startswith second arg: %v", err)
	}
	_, err = EvalApplyExpr("substr(@n,0,1)", fields)
	if err == nil || err.Error() != "Invalid type for substr. Expected string" {
		t.Fatalf("substr(@n): %v", err)
	}
	_, err = EvalApplyExpr("split(@n,',')", fields)
	if err == nil || !strings.Contains(err.Error(), "split") {
		t.Fatalf("split(@n): %v", err)
	}

	got, err = EvalApplyExpr("substr(@t,-100,2)", fields)
	if err != nil || got != "" {
		t.Fatalf("substr overshoot want empty: got %q err %v", got, err)
	}
	got, err = EvalApplyExpr("substr(@t,-5,2)", fields)
	if err != nil || got != "He" {
		t.Fatalf("substr(-5,2): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("substr(@t,-6,2)", fields)
	if err != nil || got != "" {
		t.Fatalf("substr(-6,2) want empty: got %q err %v", got, err)
	}
}
