package redisearch

import (
	"math"
	"strings"
	"testing"
)

func TestEvalApplyTimeFunctionsMatchRedis810(t *testing.T) {
	fields := map[string]interface{}{"ts": float64(1704198896)}
	tests := map[string]interface{}{
		"day(@ts)":                           float64(1704153600),
		"hour(@ts)":                          float64(1704196800),
		"minute(@ts)":                        float64(1704198840),
		"month(@ts)":                         float64(1704067200),
		"dayofweek(@ts)":                     float64(2),
		"dayofmonth(@ts)":                    float64(2),
		"dayofyear(@ts)":                     float64(1),
		"year(@ts)":                          float64(2024),
		"monthofyear(@ts)":                   float64(0),
		"timefmt(@ts)":                       "2024-01-02T12:34:56Z",
		"timefmt(@ts,'%Y-%m-%d')":            "2024-01-02",
		"timefmt(@ts,'%F')":                  "2024-01-02",
		"timefmt(@ts,'%T')":                  "12:34:56",
		"timefmt(@ts,'%y')":                  "24",
		"timefmt(@ts,'%a')":                  "Tue",
		"timefmt(@ts,'%A')":                  "Tuesday",
		"timefmt(@ts,'%b')":                  "Jan",
		"timefmt(@ts,'%B')":                  "January",
		"timefmt(@ts,'%I')":                  "12",
		"timefmt(@ts,'%p')":                  "PM",
		"timefmt(@ts,'%R')":                  "12:34",
		"timefmt(@ts,'%z')":                  "+0000",
		"timefmt(@ts,'%w')":                  "2",
		"timefmt(@ts,'%j')":                  "002",
		"timefmt(@ts,'%e')":                  " 2",
		"timefmt(@ts,'%k')":                  "12",
		"timefmt(@ts,'%X')":                  "12:34:56",
		"timefmt(@ts,'%c')":                  "Tue Jan  2 12:34:56 2024",
		"timefmt(@ts,'%Z')":                  "GMT",
		"timefmt(@ts,'hello %Q world')":      "hello %Q world",
		"parsetime('2024-01-02','%Y-%m-%d')": float64(1704153600),
		"parsetime('24-01-02','%y-%m-%d')":   float64(1704153600),
		"parsetime('Jan 02 2024','%b %d %Y')": float64(1704153600),
	}
	for expr, want := range tests {
		got, err := EvalApplyExpr(expr, fields)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		if got != want {
			t.Errorf("%s: got %v, want %v", expr, got, want)
		}
	}
	// Redis: unknown strftime directives kept literally; parsetime fail → Null.
	got, err := EvalApplyExpr("timefmt(@ts,'%Q')", fields)
	if err != nil || got != "%Q" {
		t.Fatalf("timefmt unknown directive: got %v err %v want %%Q", got, err)
	}
	got, err = EvalApplyExpr(`parsetime("nope","%Y-%m-%d")`, nil)
	if err != nil || got != nil {
		t.Fatalf("parsetime fail: got %v err %v want nil", got, err)
	}
}

func TestEvalApplyGeoDistanceMatchesRedis810(t *testing.T) {
	for _, expr := range []string{
		"geodistance('0,0','0,1')",
		"geodistance(0,0,0,1)",
	} {
		got, err := EvalApplyExpr(expr, nil)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		distance, ok := got.(float64)
		if !ok || math.Abs(distance-111226.3) > 0.1 {
			t.Errorf("%s: got %v, want about 111226.3", expr, got)
		}
	}
}

func TestEvalApplyToStrToNumber(t *testing.T) {
	fields := map[string]interface{}{"n": float64(42), "s": "7.5"}
	got, err := EvalApplyExpr("to_str(@n)", fields)
	if err != nil || got != "42" {
		t.Fatalf("to_str(@n): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("to_number(@s)", fields)
	if err != nil || got != float64(7.5) {
		t.Fatalf("to_number(@s): got %v err %v", got, err)
	}
	got, err = EvalApplyExpr("to_number(@n) + 1", fields)
	if err != nil || got != float64(43) {
		t.Fatalf("to_number(@n) + 1: got %v err %v", got, err)
	}
	_, err = EvalApplyExpr("to_number('abc')", nil)
	if err == nil || !strings.Contains(err.Error(), "SEARCH_PARSE_ARGS to_number: cannot convert string 'abc'") {
		t.Fatalf("to_number bad: %v", err)
	}
	_, err = EvalApplyExpr("nosuchfn(1)", nil)
	if err == nil || !strings.Contains(err.Error(), "SEARCH_EXPR Unknown function name 'nosuchfn'") {
		t.Fatalf("unknown fn: %v", err)
	}
	got, err = EvalApplyExpr("floor('hello')", nil)
	if err != nil || got != "nan" {
		t.Fatalf("floor non-numeric want nan: got %v err %v", got, err)
	}
}
