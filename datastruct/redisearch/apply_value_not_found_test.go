package redisearch

import (
	"strings"
	"testing"
)

func TestApplyExistsNullSemantics(t *testing.T) {
	got, err := EvalApplyExpr("exists(@price)", map[string]interface{}{"price": float64(0)})
	if err != nil || got != float64(1) {
		t.Fatalf("exists(present 0)=%v err=%v want 1", got, err)
	}
	got, err = EvalApplyExpr("exists(@missing)", map[string]interface{}{"price": float64(0)})
	if err != nil || got != float64(0) {
		t.Fatalf("exists(missing)=%v err=%v want 0", got, err)
	}
	got, err = EvalApplyExpr("exists(0)", nil)
	if err != nil || got != float64(1) {
		t.Fatalf("exists(0)=%v err=%v want 1", got, err)
	}
	got, err = EvalApplyExpr(`exists("")`, nil)
	if err != nil || got != float64(1) {
		t.Fatalf("exists(\"\")=%v err=%v want 1", got, err)
	}
}

func TestApplyValueNotFound(t *testing.T) {
	_, err := EvalApplyExpr("upper(@t)", map[string]interface{}{"n": float64(1)})
	if err == nil || !strings.Contains(err.Error(), "SEARCH_VALUE_NOT_FOUND") || !strings.Contains(err.Error(), " for t") {
		t.Fatalf("upper missing: %v", err)
	}
	_, err = EvalApplyExpr("@t", map[string]interface{}{})
	if err == nil || !isValueNotFound(err) {
		t.Fatalf("bare @t missing: %v", err)
	}
}

func TestApplyShortCircuitExistsAndCase(t *testing.T) {
	fields := map[string]interface{}{"n": float64(1)} // no t
	ok, err := EvalFilterExpr("exists(@t) && strlen(@t) > 0", fields)
	if err != nil || ok {
		t.Fatalf("&& short-circuit: ok=%v err=%v", ok, err)
	}
	got, err := EvalApplyExpr(`case(exists(@t), upper(@t), "missing")`, fields)
	if err != nil || got != "missing" {
		t.Fatalf("case short-circuit: got=%v err=%v", got, err)
	}
	fields["t"] = "hi"
	got, err = EvalApplyExpr(`case(exists(@t), upper(@t), "missing")`, fields)
	if err != nil || got != "HI" {
		t.Fatalf("case then: got=%v err=%v", got, err)
	}
}

func TestApplySubstrByteOffsets(t *testing.T) {
	// Redis substr uses bytes: "你好abc" UTF-8 → substr(0,3) == "你"
	got, err := EvalApplyExpr(`substr(@t, 0, 3)`, map[string]interface{}{"t": "你好abc"})
	if err != nil || got != "你" {
		t.Fatalf("substr bytes: got=%q err=%v", got, err)
	}
	got, err = EvalApplyExpr(`substr(@t, 0, 6)`, map[string]interface{}{"t": "你好abc"})
	if err != nil || got != "你好" {
		t.Fatalf("substr 6: got=%q err=%v", got, err)
	}
}

func TestApplyGeoDistanceThreeArg(t *testing.T) {
	fields := map[string]interface{}{"loc": "13.36,38.11"}
	got, err := EvalApplyExpr("geodistance(@loc,13.361389,38.115556)", fields)
	if err != nil {
		t.Fatalf("geo3 point,lon,lat: %v", err)
	}
	d1, ok := got.(float64)
	if !ok || d1 < 600 || d1 > 700 {
		t.Fatalf("geo3 d1=%v want ~630m", got)
	}
	got, err = EvalApplyExpr("geodistance(13.361389,38.115556,@loc)", fields)
	if err != nil {
		t.Fatalf("geo3 lon,lat,point: %v", err)
	}
	d2, ok := got.(float64)
	if !ok || d2 < 600 || d2 > 700 {
		t.Fatalf("geo3 d2=%v want ~630m", got)
	}
	if abs := d1 - d2; abs > 0.01 || abs < -0.01 {
		t.Fatalf("geo3 forms disagree: %v vs %v", d1, d2)
	}
}

func TestApplyOrShortCircuitNull(t *testing.T) {
	fields := map[string]interface{}{"n": float64(1)}
	got, err := EvalApplyExpr("1 || @t", fields)
	if err != nil {
		t.Fatalf("1 || @t: %v", err)
	}
	// bool true or numeric 1
	switch v := got.(type) {
	case bool:
		if !v {
			t.Fatalf("1 || @t want true, got false")
		}
	case float64:
		if v == 0 {
			t.Fatalf("1 || @t want nonzero, got 0")
		}
	default:
		t.Fatalf("1 || @t unexpected %#v", got)
	}
	_, err = EvalApplyExpr("0 || @t", fields)
	if err == nil || !isValueNotFound(err) {
		t.Fatalf("0 || @t want VALUE_NOT_FOUND, got %v", err)
	}
}
