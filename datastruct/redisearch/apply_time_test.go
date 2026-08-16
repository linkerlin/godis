package redisearch

import (
	"math"
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
		"parsetime('2024-01-02','%Y-%m-%d')": float64(1704153600),
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
