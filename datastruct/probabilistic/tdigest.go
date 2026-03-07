package probabilistic

import (
	"math"
	"sort"
)

// Centroid represents a cluster in T-Digest
type Centroid struct {
	Mean   float64
	Weight float64
}

// TDigest is a T-Digest data structure for accurate quantile estimation
type TDigest struct {
	centroids []Centroid
	compression float64
	count     float64
	sum       float64
	sumSq     float64
	min       float64
	max       float64
}

// NewTDigest creates a new T-Digest
func NewTDigest(compression float64) *TDigest {
	if compression <= 0 {
		compression = 100
	}
	
	return &TDigest{
		centroids:   make([]Centroid, 0),
		compression: compression,
		min:         math.Inf(1),
		max:         math.Inf(-1),
	}
}

// Add adds a value to the digest
func (td *TDigest) Add(value float64, weight float64) {
	if weight <= 0 {
		weight = 1
	}
	
	td.centroids = append(td.centroids, Centroid{Mean: value, Weight: weight})
	
	td.count += weight
	td.sum += value * weight
	td.sumSq += value * value * weight
	
	if value < td.min {
		td.min = value
	}
	if value > td.max {
		td.max = value
	}
	
	// Compress if too many centroids
	if len(td.centroids) > int(10 * td.compression) {
		td.Compress()
	}
}

// Quantile returns the estimated quantile (0 <= q <= 1)
func (td *TDigest) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		return math.NaN()
	}
	
	if len(td.centroids) == 0 {
		return math.NaN()
	}
	
	if q == 0 {
		return td.min
	}
	if q == 1 {
		return td.max
	}
	
	// Sort centroids by mean
	sorted := make([]Centroid, len(td.centroids))
	copy(sorted, td.centroids)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Mean < sorted[j].Mean
	})
	
	// Find the quantile
	target := q * td.count
	cumSum := 0.0
	
	for i, c := range sorted {
		cumSum += c.Weight
		if cumSum >= target {
			if i == 0 {
				return c.Mean
			}
			// Interpolate between centroids
			prev := sorted[i-1]
			prevCum := cumSum - c.Weight
			t := (target - prevCum) / c.Weight
			return prev.Mean + t*(c.Mean-prev.Mean)
		}
	}
	
	return sorted[len(sorted)-1].Mean
}

// CDF returns the cumulative distribution function value
func (td *TDigest) CDF(value float64) float64 {
	if len(td.centroids) == 0 {
		return math.NaN()
	}
	
	if value <= td.min {
		return 0
	}
	if value >= td.max {
		return 1
	}
	
	// Sort centroids
	sorted := make([]Centroid, len(td.centroids))
	copy(sorted, td.centroids)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Mean < sorted[j].Mean
	})
	
	// Find CDF
	cumSum := 0.0
	for i, c := range sorted {
		if c.Mean >= value {
			if i == 0 {
				return 0
			}
			// Interpolate
			prev := sorted[i-1]
			t := (value - prev.Mean) / (c.Mean - prev.Mean)
			return (cumSum + t*c.Weight) / td.count
		}
		cumSum += c.Weight
	}
	
	return 1
}

// Compress merges centroids to reduce memory
func (td *TDigest) Compress() {
	if len(td.centroids) < 2 {
		return
	}
	
	// Sort centroids
	sort.Slice(td.centroids, func(i, j int) bool {
		return td.centroids[i].Mean < td.centroids[j].Mean
	})
	
	// Merge close centroids
	newCentroids := make([]Centroid, 0, len(td.centroids))
	current := td.centroids[0]
	
	for i := 1; i < len(td.centroids); i++ {
		next := td.centroids[i]
		
		// Check if we can merge
		// Use k-size limit: max weight for a centroid at position i
		q := (float64(len(newCentroids)) + 0.5) / float64(len(td.centroids))
		k := 4 * td.compression * q * (1 - q)
		
		if current.Weight+next.Weight <= k {
			// Merge
			current = Centroid{
				Mean:   (current.Mean*current.Weight + next.Mean*next.Weight) / (current.Weight + next.Weight),
				Weight: current.Weight + next.Weight,
			}
		} else {
			newCentroids = append(newCentroids, current)
			current = next
		}
	}
	newCentroids = append(newCentroids, current)
	
	td.centroids = newCentroids
}

// Min returns the minimum value
func (td *TDigest) Min() float64 {
	return td.min
}

// Max returns the maximum value
func (td *TDigest) Max() float64 {
	return td.max
}

// Mean returns the mean
func (td *TDigest) Mean() float64 {
	if td.count == 0 {
		return math.NaN()
	}
	return td.sum / td.count
}

// StdDev returns the standard deviation
func (td *TDigest) StdDev() float64 {
	if td.count == 0 {
		return math.NaN()
	}
	mean := td.Mean()
	variance := (td.sumSq / td.count) - (mean * mean)
	if variance < 0 {
		variance = 0
	}
	return math.Sqrt(variance)
}

// Info returns T-Digest info
func (td *TDigest) Info() map[string]interface{} {
	return map[string]interface{}{
		"compression": td.compression,
		"centroids":   len(td.centroids),
		"count":       td.count,
		"min":         td.min,
		"max":         td.max,
		"mean":        td.Mean(),
		"stdDev":      td.StdDev(),
	}
}
