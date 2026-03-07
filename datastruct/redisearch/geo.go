package redisearch

import (
	"fmt"
	"math"
)

// GeoPoint represents a geographic coordinate
type GeoPoint struct {
	Lat float64
	Lon float64
}

// HaversineDistance calculates the distance between two points in kilometers
func HaversineDistance(p1, p2 GeoPoint) float64 {
	const R = 6371 // Earth's radius in kilometers
	
	lat1Rad := p1.Lat * math.Pi / 180
	lat2Rad := p2.Lat * math.Pi / 180
	deltaLat := (p2.Lat - p1.Lat) * math.Pi / 180
	deltaLon := (p2.Lon - p1.Lon) * math.Pi / 180
	
	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	
	return R * c
}

// ParseGeoPoint parses a geo point from string
// Format: "lon,lat"
func ParseGeoPoint(s string) (GeoPoint, bool) {
	var p GeoPoint
	_, err := fmt.Sscanf(s, "%f,%f", &p.Lon, &p.Lat)
	if err != nil {
		// Try lat,lon format
		_, err = fmt.Sscanf(s, "%f,%f", &p.Lat, &p.Lon)
		if err != nil {
			return p, false
		}
		return GeoPoint{Lat: p.Lon, Lon: p.Lat}, true
	}
	return p, true
}

// GeoFilter represents a geographic filter
type GeoFilter struct {
	Field    string
	Center   GeoPoint
	Radius   float64
	Unit     string // m, km, mi, ft
}

// Matches checks if a point matches the filter
func (gf *GeoFilter) Matches(point GeoPoint) bool {
	distance := HaversineDistance(gf.Center, point)
	
	// Convert to filter unit
	switch gf.Unit {
	case "m":
		distance *= 1000
	case "mi":
		distance *= 0.621371
	case "ft":
		distance *= 3280.84
	}
	
	return distance <= gf.Radius
}

// GeoIndex provides spatial indexing for documents
type GeoIndex struct {
	points map[string]GeoPoint // docID -> point
}

// NewGeoIndex creates a new geo index
func NewGeoIndex() *GeoIndex {
	return &GeoIndex{
		points: make(map[string]GeoPoint),
	}
}

// Add adds a document to the geo index
func (gi *GeoIndex) Add(docID string, point GeoPoint) {
	gi.points[docID] = point
}

// Remove removes a document from the geo index
func (gi *GeoIndex) Remove(docID string) {
	delete(gi.points, docID)
}

// Search finds documents within radius of center
func (gi *GeoIndex) Search(center GeoPoint, radius float64, unit string) []string {
	var results []string
	
	for docID, point := range gi.points {
		filter := &GeoFilter{
			Center: center,
			Radius: radius,
			Unit:   unit,
		}
		if filter.Matches(point) {
			results = append(results, docID)
		}
	}
	
	return results
}


