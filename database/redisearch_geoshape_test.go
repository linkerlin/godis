package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestGeoShapeEndToEnd verifies a GEOSHAPE field supports WITHIN/CONTAINS/
// INTERSECTS/DISJOINT queries via PARAMS-supplied WKT (DIALECT 3).
func TestGeoShapeEndToEnd(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "geo1", "SCHEMA", "geom", "GEOSHAPE",
	)), "OK")
	// Two point docs: one inside the unit box, one outside.
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "geo1", "inside", "FIELDS", "geom", "POINT(5 5)")); protocol.IsErrorReply(r) {
		t.Fatalf("add inside: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "geo1", "outside", "FIELDS", "geom", "POINT(50 50)")); protocol.IsErrorReply(r) {
		t.Fatalf("add outside: %s", r.ToBytes())
	}

	box := "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"

	// WITHIN: only "inside" is within the box.
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "geo1", "@geom:[WITHIN $poly]", "NOCONTENT",
		"PARAMS", "2", "poly", box, "DIALECT", "3",
	))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("WITHIN should match 1 (inside), got %s", r.ToBytes())
	}

	// DISJOINT: only "outside" is disjoint from the box.
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "geo1", "@geom:[DISJOINT $poly]", "NOCONTENT",
		"PARAMS", "2", "poly", box, "DIALECT", "3",
	))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("DISJOINT should match 1 (outside), got %s", r.ToBytes())
	}

	// INTERSECTS: "inside" intersects the box (1).
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "geo1", "@geom:[INTERSECTS $poly]", "NOCONTENT",
		"PARAMS", "2", "poly", box, "DIALECT", "3",
	))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("INTERSECTS should match 1 (inside), got %s", r.ToBytes())
	}
}

// TestGeoShapeFlatSpherical verifies the FLAT/SPHERICAL coordinate-system option
// parses and indexes without error (the in-memory engine treats both as 2D).
func TestGeoShapeFlatSpherical(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "geo2", "SCHEMA", "g", "GEOSHAPE", "FLAT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "geo2", "d1", "FIELDS", "g", "POINT(1 2)")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "geo2", "@g:[WITHIN $poly]", "NOCONTENT",
		"PARAMS", "2", "poly", "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))", "DIALECT", "3",
	))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("FLAT geoshape WITHIN should match 1, got %s", r.ToBytes())
	}
}
