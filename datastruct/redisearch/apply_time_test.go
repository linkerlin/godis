package redisearch

import (
	"math"
	"strings"
	"testing"
)

func TestEvalApplyTimeFunctionsMatchRedis810(t *testing.T) {
	fields := map[string]interface{}{"ts": float64(1704198896)}
	tests := map[string]interface{}{
		"day(@ts)":                            float64(1704153600),
		"hour(@ts)":                           float64(1704196800),
		"minute(@ts)":                         float64(1704198840),
		"month(@ts)":                          float64(1704067200),
		"dayofweek(@ts)":                      float64(2),
		"dayofmonth(@ts)":                     float64(2),
		"dayofyear(@ts)":                      float64(1),
		"year(@ts)":                           float64(2024),
		"monthofyear(@ts)":                    float64(0),
		"timefmt(@ts)":                        "2024-01-02T12:34:56Z",
		"timefmt(@ts,'%Y-%m-%d')":             "2024-01-02",
		"timefmt(@ts,'%F')":                   "2024-01-02",
		"timefmt(@ts,'%T')":                   "12:34:56",
		"timefmt(@ts,'%y')":                   "24",
		"timefmt(@ts,'%a')":                   "Tue",
		"timefmt(@ts,'%A')":                   "Tuesday",
		"timefmt(@ts,'%b')":                   "Jan",
		"timefmt(@ts,'%h')":                   "Jan",
		"timefmt(@ts,'%B')":                   "January",
		"timefmt(@ts,'%I')":                   "12",
		"timefmt(@ts,'%p')":                   "PM",
		"timefmt(@ts,'%P')":                   "pm",
		"timefmt(@ts,'%k')":                   "12",
		"timefmt(@ts,'%l')":                   "12",
		"timefmt(@ts,'%R')":                   "12:34",
		"timefmt(@ts,'%z')":                   "+0000",
		"timefmt(@ts,'%Z')":                   "UTC",
		"timefmt(@ts,'%w')":                   "2",
		"timefmt(@ts,'%j')":                   "002",
		"timefmt(@ts,'%e')":                   " 2",
		"timefmt(@ts,'%s')":                   "1704198896",
		"timefmt(@ts,'%D')":                   "01/02/24",
		"timefmt(@ts,'%r')":                   "12:34:56 PM",
		"timefmt(@ts,'%c')":                   "Tue Jan  2 12:34:56 2024",
		"timefmt(@ts,'%C')":                   "20",
		"timefmt(@ts,'%u')":                   "2",
		"timefmt(@ts,'%U')":                   "00",
		"timefmt(@ts,'%W')":                   "01",
		"timefmt(@ts,'%V')":                   "01",
		"timefmt(@ts,'%G')":                   "2024",
		"timefmt(@ts,'%g')":                   "24",
		"timefmt(@ts,'%X')":                   "12:34:56",
		"timefmt(@ts,'%x')":                   "01/02/24",
		"parsetime('2024-01-02','%Y-%m-%d')":  float64(1704153600),
		"parsetime('24-01-02','%y-%m-%d')":    float64(1704153600),
		"parsetime('Jan 02 2024','%b %d %Y')": float64(1704153600),
		"parsetime('01/02/24','%D')":          float64(1704153600),
		"parsetime('Jan 02 2024','%h %d %Y')": float64(1704153600),
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
	// Redis 8.x QE: unsupported directive → Null (not literal keep).
	for _, bad := range []string{"timefmt(@ts,'%Q')", "timefmt(@ts,'hello %Q world')"} {
		got, err := EvalApplyExpr(bad, fields)
		if err != nil || got != nil {
			t.Fatalf("%s: got %v err %v want nil", bad, got, err)
		}
	}
	// Redis 8.6 QE: %k/%l space-padded; %P lowercase am/pm (midnight / 13:00).
	for _, tc := range []struct {
		unix int64
		expr string
		want string
	}{
		{1704153600, "timefmt(@ts,'%k|%l|%P|%p')", " 0|12|am|AM"},
		{1704157200, "timefmt(@ts,'%k|%l|%P')", " 1| 1|am"},
		{1704200400, "timefmt(@ts,'%k|%l|%P|%p')", "13| 1|pm|PM"},
		{1704236400, "timefmt(@ts,'%k|%l|%P')", "23|11|pm"},
	} {
		got, err := EvalApplyExpr(tc.expr, map[string]interface{}{"ts": float64(tc.unix)})
		if err != nil || got != tc.want {
			t.Fatalf("ts=%d %s: got %v err %v want %s", tc.unix, tc.expr, got, err, tc.want)
		}
	}
	got, err := EvalApplyExpr(`parsetime("nope","%Y-%m-%d")`, nil)
	if err != nil || got != nil {
		t.Fatalf("parsetime fail: got %v err %v want nil", got, err)
	}
	got, err = EvalApplyExpr("timefmt(@ts,'A%nB%tC')", fields)
	if err != nil || got != "A\nB\tC" {
		t.Fatalf("timefmt %%n/%%t: got %q err %v", got, err)
	}
}

func TestStrftimeWeekNumsMatchRedis810(t *testing.T) {
	cases := []struct {
		unix                   int64
		u, U, W, V, G, g, w, C string
	}{
		{1704067200, "1", "00", "01", "01", "2024", "24", "1", "20"}, // Mon Jan 1 2024
		{1704585600, "7", "01", "01", "01", "2024", "24", "0", "20"}, // Sun Jan 7 2024
		{1704024000, "7", "53", "52", "52", "2023", "23", "0", "20"}, // Sun Dec 31 2023 12:00
		{1735689600, "3", "00", "00", "01", "2025", "25", "3", "20"}, // Wed Jan 1 2025
	}
	for _, c := range cases {
		fields := map[string]interface{}{"ts": float64(c.unix)}
		expr := "timefmt(@ts,'%u|%U|%W|%V|%G|%g|%w|%C')"
		want := strings.Join([]string{c.u, c.U, c.W, c.V, c.G, c.g, c.w, c.C}, "|")
		got, err := EvalApplyExpr(expr, fields)
		if err != nil || got != want {
			t.Errorf("ts=%d: got %v err %v want %s", c.unix, got, err, want)
		}
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
