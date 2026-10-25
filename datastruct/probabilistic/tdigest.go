package probabilistic

import (
	"encoding/json"
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
	centroids   []Centroid // merged (compressed) centroids
	unmerged   []Centroid // raw buffered observations (compressed on query)
	compression float64
	count       float64
	sum         float64
	sumSq       float64
	min         float64
	max         float64
	compressions int64 // compress runs that did work (real 8.10 semantics)
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

	// Real 8.10 buffers observations unmerged; queries compress them.
	td.unmerged = append(td.unmerged, Centroid{Mean: value, Weight: weight})

	td.count += weight
	td.sum += value * weight
	td.sumSq += value * value * weight

	if value < td.min {
		td.min = value
	}
	if value > td.max {
		td.max = value
	}

	// Auto-compress when the raw buffer outgrows the compression budget.
	if len(td.unmerged) > int(10*td.compression) {
		td.Compress()
	}
}

// Quantile returns the estimated quantile (0 <= q <= 1). Centroid walk as
// real 8.10: the mean of the first centroid whose cumulative weight strictly
// exceeds q*n (falls through to max at q=1).
func (td *TDigest) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		return math.NaN()
	}
	td.ensureCompressed()
	if len(td.centroids) == 0 {
		return math.NaN()
	}

	sorted := make([]Centroid, len(td.centroids))
	copy(sorted, td.centroids)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Mean < sorted[j].Mean
	})

	td.ensureCompressed()
	target := q * td.count
	cum := 0.0
	for _, c := range sorted {
		cum += c.Weight
		if cum > target {
			return c.Mean
		}
	}
	return td.max
}

// CDF returns the cumulative distribution function value: (weight strictly
// below + half the weight of the centroid holding the value) / n (probed
// against real 8.10; outside [min,max] clamps to 0/1).
func (td *TDigest) CDF(value float64) float64 {
	td.ensureCompressed()
	if len(td.centroids) == 0 {
		return math.NaN()
	}
	if value < td.min {
		return 0
	}
	if value > td.max {
		return 1
	}

	sorted := make([]Centroid, len(td.centroids))
	copy(sorted, td.centroids)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Mean < sorted[j].Mean
	})

	below, equal := 0.0, 0.0
	for _, c := range sorted {
		if c.Mean < value {
			below += c.Weight
		} else if c.Mean == value {
			equal += c.Weight
		} else {
			break
		}
	}
	if equal > 0 {
		return (below + equal/2) / td.count
	}
	return below / td.count
}

// Compress folds the unmerged buffer into the merged centroid list (real
// 8.10: a compress run with an empty buffer is a no-op and not counted).
func (td *TDigest) Compress() {
	if len(td.unmerged) == 0 {
		return
	}
	td.centroids = append(td.centroids, td.unmerged...)
	td.unmerged = nil
	td.compressions++
}

// ensureCompressed makes query results see buffered observations.
func (td *TDigest) ensureCompressed() {
	td.Compress()
}

// Min returns the minimum value
func (td *TDigest) Min() float64 {
	return td.min
}

// Max returns the maximum value
func (td *TDigest) Max() float64 {
	return td.max
}

// Reset clears all samples from the digest
func (td *TDigest) Reset() {
	td.centroids = td.centroids[:0]
	td.unmerged = nil
	td.compressions = 0
	td.count = 0
	td.sum = 0
	td.sumSq = 0
	td.min = math.Inf(1)
	td.max = math.Inf(-1)
}

// Rank returns how many samples are strictly less than value (approx via CDF).
func (td *TDigest) Rank(value float64) int64 {
	if td.count == 0 {
		return -2
	}
	return int64(td.CDF(value) * td.count)
}

// RevRank returns how many samples are strictly greater than value (approx).
func (td *TDigest) RevRank(value float64) int64 {
	if td.count == 0 {
		return -2
	}
	return int64((1 - td.CDF(value)) * td.count)
}

// ByRank returns the value at the given 0-based rank (approx via quantile).
func (td *TDigest) ByRank(rank int64) float64 {
	if td.count == 0 || rank < 0 {
		return math.NaN()
	}
	if float64(rank) >= td.count {
		return td.max
	}
	if td.count == 1 {
		return td.min
	}
	q := float64(rank) / (td.count - 1)
	return td.Quantile(q)
}

// ByRevRank returns the value at reverse rank (0 = max).
func (td *TDigest) ByRevRank(rank int64) float64 {
	if td.count == 0 || rank < 0 {
		return math.NaN()
	}
	if float64(rank) >= td.count {
		return td.min
	}
	return td.ByRank(int64(td.count) - 1 - rank)
}

// TrimmedMean returns mean of values between lowCut and highCut quantiles (0..1).
func (td *TDigest) TrimmedMean(lowCut, highCut float64) float64 {
	if td.count == 0 || lowCut < 0 || highCut > 1 || lowCut >= highCut {
		return math.NaN()
	}
	lo := td.Quantile(lowCut)
	hi := td.Quantile(highCut)
	var sum, w float64
	for _, c := range td.centroids {
		if c.Mean >= lo && c.Mean <= hi {
			sum += c.Mean * c.Weight
			w += c.Weight
		}
	}
	if w == 0 {
		return math.NaN()
	}
	return sum / w
}

// MergeFrom merges another digest into this one.
func (td *TDigest) MergeFrom(other *TDigest) {
	if other == nil {
		return
	}
	for _, c := range other.centroids {
		td.Add(c.Mean, c.Weight)
	}
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

// Compression returns the configured compression parameter.
func (td *TDigest) Compression() float64 { return td.compression }

// Info returns the real 8.10 TDIGEST.INFO field set in wire order.
// Capacity = 6*compression+10 and Memory usage = 16*capacity+80 reproduce
// the C implementation's formulas (probed across compressions 1..500).
func (td *TDigest) Info() [][2]interface{} {
	capacity := int64(6*td.compression) + 10
	mergedWeight, unmergedWeight := 0.0, 0.0
	for _, c := range td.centroids {
		mergedWeight += c.Weight
	}
	for _, c := range td.unmerged {
		unmergedWeight += c.Weight
	}
	return [][2]interface{}{
		{"Compression", int64(td.compression)},
		{"Capacity", capacity},
		{"Merged nodes", int64(len(td.centroids))},
		{"Unmerged nodes", int64(len(td.unmerged))},
		{"Merged weight", int64(mergedWeight)},
		{"Unmerged weight", int64(unmergedWeight)},
		{"Observations", int64(td.count)},
		{"Total compressions", td.compressions},
		{"Memory usage", 16*capacity + 80},
	}
}

type tdigestWire struct {
	Centroids   []Centroid `json:"c"`
	Compression float64    `json:"comp"`
	Count       float64    `json:"n"`
	Sum         float64    `json:"sum"`
	SumSq       float64    `json:"sumsq"`
	Min         float64    `json:"min"`
	Max         float64    `json:"max"`
}

// EncodeJSON serializes T-Digest for Godis opaque DUMP/RESTORE.
func (td *TDigest) EncodeJSON() ([]byte, error) {
	return json.Marshal(tdigestWire{
		Centroids:   td.centroids,
		Compression: td.compression,
		Count:       td.count,
		Sum:         td.sum,
		SumSq:       td.sumSq,
		Min:         td.min,
		Max:         td.max,
	})
}

// DecodeTDigest restores T-Digest from EncodeJSON output.
func DecodeTDigest(data []byte) (*TDigest, error) {
	var w tdigestWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.Compression <= 0 {
		w.Compression = 100
	}
	return &TDigest{
		centroids:   w.Centroids,
		compression: w.Compression,
		count:       w.Count,
		sum:         w.Sum,
		sumSq:       w.SumSq,
		min:         w.Min,
		max:         w.Max,
	}, nil
}
