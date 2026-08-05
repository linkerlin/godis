package redisearch

import "testing"

// TestGeoWKTParse verifies WKT parsing for the supported shape types.
func TestGeoWKTParse(t *testing.T) {
	cases := []struct {
		wkt      string
		kind     GeoShapeKind
		wantVert int
	}{
		{"POINT(1 2)", GeoShapePoint, 1},
		{"MULTIPOINT((1 2), (3 4))", GeoShapeMultiPoint, 2},
		{"LINESTRING(0 0, 1 1, 2 2)", GeoShapeLineString, 3},
		{"POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))", GeoShapePolygon, 5},
	}
	for _, c := range cases {
		g, err := ParseWKT(c.wkt)
		if err != nil {
			t.Fatalf("ParseWKT(%q): %v", c.wkt, err)
		}
		if g.Kind != c.kind {
			t.Fatalf("%q kind = %v, want %v", c.wkt, g.Kind, c.kind)
		}
		if len(g.vertices()) != c.wantVert {
			t.Fatalf("%q vertices = %d, want %d", c.wkt, len(g.vertices()), c.wantVert)
		}
	}
}

// TestGeoShapeRelate verifies the four spatial predicates on a unit square.
var (
	square = &GeoShape{Kind: GeoShapePolygon, Rings: [][][2]float64{{{0, 0}, {0, 10}, {10, 10}, {10, 0}, {0, 0}}}}
	inside = &GeoShape{Kind: GeoShapePoint, Point: [2]float64{5, 5}}
	outside = &GeoShape{Kind: GeoShapePoint, Point: [2]float64{20, 20}}
)

func TestGeoShapeRelate(t *testing.T) {
	// WITHIN: inside point is within the square; outside is not.
	if !RelateGeoShape(inside, square, "WITHIN") {
		t.Fatalf("point inside should be WITHIN square")
	}
	if RelateGeoShape(outside, square, "WITHIN") {
		t.Fatalf("point outside should NOT be WITHIN square")
	}
	// CONTAINS: square contains the inside point.
	if !RelateGeoShape(square, inside, "CONTAINS") {
		t.Fatalf("square should CONTAIN the inside point")
	}
	if RelateGeoShape(square, outside, "CONTAINS") {
		t.Fatalf("square should NOT CONTAIN the outside point")
	}
	// INTERSECTS / DISJOINT.
	if !RelateGeoShape(inside, square, "INTERSECTS") {
		t.Fatalf("inside point INTERSECTS square")
	}
	if !RelateGeoShape(outside, square, "DISJOINT") {
		t.Fatalf("outside point is DISJOINT from square")
	}
}
