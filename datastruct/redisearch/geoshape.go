package redisearch

import (
	"fmt"
	"strconv"
	"strings"
)

// GeoShapeKind classifies a parsed WKT shape.
type GeoShapeKind int

const (
	GeoShapePoint GeoShapeKind = iota
	GeoShapeLineString
	GeoShapePolygon
	GeoShapeMultiPoint
)

// GeoShape is a parsed WKT geometry. Polygon rings are stored as ordered vertex
// lists (outer ring first). All predicates operate on the 2D coordinates; the
// FLAT/SPHERICAL distinction (set at FT.CREATE) is informational for this
// in-memory engine — Redis uses it to pick geodesic vs planar math, but both
// reduce to 2D coordinate tests here.
type GeoShape struct {
	Kind  GeoShapeKind
	Point [2]float64            // valid when Kind == GeoShapePoint
	Rings [][][2]float64        // Polygon: [outer, hole1, ...]; LineString: [pts]; MultiPoint: each ring is one point
}

// ParseWKT parses a Well-Known-Text geometry string into a GeoShape. Supported
// types: POINT, MULTIPOINT, LINESTRING, POLYGON. Coordinates are (x, y) pairs
// (Redis GEOSHAPE uses lon, lat for SPHERICAL, x, y for FLAT).
func ParseWKT(wkt string) (*GeoShape, error) {
	s := strings.TrimSpace(wkt)
	upper := strings.ToUpper(s)
	switch {
	case strings.HasPrefix(upper, "POINT"):
		body := extractParens(s[len("POINT"):])
		xy, err := parseCoordPair(body)
		if err != nil {
			return nil, err
		}
		return &GeoShape{Kind: GeoShapePoint, Point: xy}, nil
	case strings.HasPrefix(upper, "MULTIPOINT"):
		body := extractParens(s[len("MULTIPOINT"):])
		pts, err := parsePointList(body)
		if err != nil {
			return nil, err
		}
		return &GeoShape{Kind: GeoShapeMultiPoint, Rings: pts}, nil
	case strings.HasPrefix(upper, "LINESTRING"):
		body := extractParens(s[len("LINESTRING"):])
		pts, err := parseCoordList(body)
		if err != nil {
			return nil, err
		}
		return &GeoShape{Kind: GeoShapeLineString, Rings: [][][2]float64{pts}}, nil
	case strings.HasPrefix(upper, "POLYGON"):
		body := extractParens(s[len("POLYGON"):])
		rings, err := parsePolygonRings(body)
		if err != nil {
			return nil, err
		}
		return &GeoShape{Kind: GeoShapePolygon, Rings: rings}, nil
	}
	return nil, fmt.Errorf("unsupported WKT type: %s", wkt)
}

// extractParens returns the content between the first '(' and its matching ')'.
func extractParens(s string) string {
	s = strings.TrimSpace(s)
	start := strings.Index(s, "(")
	end := strings.LastIndex(s, ")")
	if start < 0 || end < 0 || end <= start {
		return ""
	}
	return s[start+1 : end]
}

// parseCoordPair parses "x y" into a [2]float64.
func parseCoordPair(s string) ([2]float64, error) {
	fields := strings.Fields(s)
	if len(fields) != 2 {
		return [2]float64{}, fmt.Errorf("expected 2 coordinates, got %d", len(fields))
	}
	x, err1 := strconv.ParseFloat(fields[0], 64)
	y, err2 := strconv.ParseFloat(fields[1], 64)
	if err1 != nil || err2 != nil {
		return [2]float64{}, fmt.Errorf("invalid coordinate pair %q", s)
	}
	return [2]float64{x, y}, nil
}

// parseCoordList parses "x1 y1, x2 y2, ..." into a vertex slice.
func parseCoordList(s string) ([][2]float64, error) {
	parts := strings.Split(s, ",")
	pts := make([][2]float64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		xy, err := parseCoordPair(p)
		if err != nil {
			return nil, err
		}
		pts = append(pts, xy)
	}
	if len(pts) < 2 {
		return nil, fmt.Errorf("need at least 2 coordinates")
	}
	return pts, nil
}

// parsePointList parses a MULTIPOINT body. Two WKT forms are accepted:
// "x1 y1, x2 y2" and "(x1 y1), (x2 y2)".
func parsePointList(s string) ([][][2]float64, error) {
	// Split on commas that separate points, tolerating inner parens.
	cleaned := strings.NewReplacer("(", "", ")", "").Replace(s)
	parts := strings.Split(cleaned, ",")
	out := make([][][2]float64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		xy, err := parseCoordPair(p)
		if err != nil {
			return nil, err
		}
		out = append(out, [][][2]float64{{xy}}...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty multipoint")
	}
	return out, nil
}

// parsePolygonRings parses a POLYGON body: "(x y, x y, ...), (hole, ...)".
func parsePolygonRings(s string) ([][][2]float64, error) {
	var rings [][][2]float64
	// Each ring is wrapped in parens; split on ")(" boundaries.
	depth := 0
	var cur strings.Builder
	flush := func() error {
		body := strings.TrimSpace(cur.String())
		cur.Reset()
		body = strings.Trim(body, "(), ")
		if body == "" {
			return nil
		}
		pts, err := parseCoordList(body)
		if err != nil {
			return err
		}
		rings = append(rings, pts)
		return nil
	}
	for _, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		default:
			if depth > 0 {
				cur.WriteRune(r)
			}
		}
	}
	if len(rings) == 0 {
		return nil, fmt.Errorf("polygon has no rings")
	}
	return rings, nil
}

// vertices returns the shape's points as a flat list (for bbox / containment).
func (g *GeoShape) vertices() [][2]float64 {
	switch g.Kind {
	case GeoShapePoint:
		return [][2]float64{g.Point}
	case GeoShapeMultiPoint:
		var out [][2]float64
		for _, ring := range g.Rings {
			out = append(out, ring...)
		}
		return out
	default:
		var out [][2]float64
		for _, ring := range g.Rings {
			out = append(out, ring...)
		}
		return out
	}
}

// rings returns the closed rings for edge-intersection tests (polygon /
// linestring). A polygon's outer ring is Rings[0].
func (g *GeoShape) edges() [][2][2]float64 {
	var out [][2][2]float64
	addRing := func(pts [][2]float64) {
		n := len(pts)
		for i := 0; i < n; i++ {
			out = append(out, [2][2]float64{pts[i], pts[(i+1)%n]})
		}
	}
	switch g.Kind {
	case GeoShapePolygon:
		if len(g.Rings) > 0 {
			addRing(g.Rings[0])
		}
	case GeoShapeLineString:
		if len(g.Rings) > 0 {
			pts := g.Rings[0]
			for i := 0; i+1 < len(pts); i++ {
				out = append(out, [2][2]float64{pts[i], pts[i+1]})
			}
		}
	case GeoShapeMultiPoint:
		// Points have no edges.
	}
	return out
}

// pointInPolygon reports whether p lies inside the polygon's outer ring (holes
// excluded — a point in a hole is NOT inside the polygon). Ray-casting algorithm.
func pointInPolygon(p [2]float64, ring [][2]float64) bool {
	n := len(ring)
	if n < 3 {
		return false
	}
	inside := false
	j := n - 1
	for i := 0; i < n; i++ {
		xi, yi := ring[i][0], ring[i][1]
		xj, yj := ring[j][0], ring[j][1]
		if (yi > p[1]) != (yj > p[1]) {
			xIntersect := (xj-xi)*(p[1]-yi)/(yj-yi) + xi
			if p[0] < xIntersect {
				inside = !inside
			}
		}
		j = i
	}
	return inside
}

// segmentsIntersect reports whether segment a-b crosses c-d.
func segmentsIntersect(a, b, c, d [2]float64) bool {
	ccw := func(p, q, r [2]float64) int {
		v := (r[1]-p[1])*(q[0]-p[0]) - (q[1]-p[1])*(r[0]-p[0])
		if v > 0 {
			return 1
		}
		if v < 0 {
			return -1
		}
		return 0
	}
	d1 := ccw(c, d, a)
	d2 := ccw(c, d, b)
	d3 := ccw(a, b, c)
	d4 := ccw(a, b, d)
	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}
	return false
}

// containsPoint reports whether p is inside the GeoShape (polygon outer ring,
// excluding holes; or exactly equal to a Point/multipoint vertex).
func (g *GeoShape) containsPoint(p [2]float64) bool {
	switch g.Kind {
	case GeoShapePoint:
		return g.Point == p
	case GeoShapeMultiPoint:
		for _, ring := range g.Rings {
			for _, v := range ring {
				if v == p {
					return true
				}
			}
		}
		return false
	case GeoShapePolygon:
		if len(g.Rings) == 0 {
			return false
		}
		if !pointInPolygon(p, g.Rings[0]) {
			return false
		}
		for _, hole := range g.Rings[1:] {
			if pointInPolygon(p, hole) {
				return false
			}
		}
		return true
	case GeoShapeLineString:
		// A linestring "contains" a point only if the point lies on it; skip the
		// on-segment test (rare query), treat as not-containing.
		return false
	}
	return false
}

// edgesCross reports whether any edge of a intersects any edge of b.
func edgesCross(a, b *GeoShape) bool {
	ea, eb := a.edges(), b.edges()
	for _, sa := range ea {
		for _, sb := range eb {
			if segmentsIntersect(sa[0], sa[1], sb[0], sb[1]) {
				return true
			}
		}
	}
	return false
}

// RelateGeoShape evaluates the named spatial predicate between a document shape
// and a query shape. Used by GeoShapeNode.Evaluate.
//
//	WITHIN     — every doc vertex is inside query, and no edges cross out
//	CONTAINS   — every query vertex is inside doc, and no edges cross out
//	INTERSECTS — any vertex containment OR any edge crossing
//	DISJOINT   — not INTERSECTS
func RelateGeoShape(doc, query *GeoShape, op string) bool {
	switch strings.ToUpper(op) {
	case "WITHIN":
		// doc is within query: all doc vertices inside query, no edge crossing.
		for _, v := range doc.vertices() {
			if !query.containsPoint(v) {
				return false
			}
		}
		if edgesCross(doc, query) {
			return false
		}
		return true
	case "CONTAINS":
		// doc contains query: reverse of WITHIN.
		for _, v := range query.vertices() {
			if !doc.containsPoint(v) {
				return false
			}
		}
		if edgesCross(doc, query) {
			return false
		}
		return true
	case "INTERSECTS":
		for _, v := range doc.vertices() {
			if query.containsPoint(v) {
				return true
			}
		}
		for _, v := range query.vertices() {
			if doc.containsPoint(v) {
				return true
			}
		}
		return edgesCross(doc, query)
	case "DISJOINT":
		return !RelateGeoShape(doc, query, "INTERSECTS")
	}
	return false
}
