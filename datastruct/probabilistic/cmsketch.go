package probabilistic

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
)

// CountMinSketch is a Count-Min Sketch implementation
type CountMinSketch struct {
	table     [][]uint64
	width     uint
	depth     uint
	count     uint64
}

// NewCountMinSketch creates a new Count-Min Sketch
// width: number of columns
// depth: number of hash functions (rows)
func NewCountMinSketch(width, depth uint) *CountMinSketch {
	table := make([][]uint64, depth)
	for i := range table {
		table[i] = make([]uint64, width)
	}
	
	return &CountMinSketch{
		table: table,
		width: width,
		depth: depth,
	}
}

// NewCountMinSketchFromError creates a CMS from desired error rate and confidence
// errorRate: acceptable error rate (e.g., 0.001 for 0.1%)
// confidence: probability of being within error rate (e.g., 0.99 for 99%)
func NewCountMinSketchFromError(errorRate float64, confidence float64) *CountMinSketch {
	// width = ceil(e / errorRate)
	// depth = ceil(ln(1 / (1 - confidence)))
	width := uint(math.Ceil(math.E / errorRate))
	depth := uint(math.Ceil(math.Log(1 / (1 - confidence))))
	
	if width < 100 {
		width = 100
	}
	if depth < 2 {
		depth = 2
	}
	
	return NewCountMinSketch(width, depth)
}

// IncrBy increments the count for an item by a given amount
func (cms *CountMinSketch) IncrBy(item []byte, increment uint64) {
	for i := uint(0); i < cms.depth; i++ {
		idx := cms.hash(item, i)
		cms.table[i][idx] += increment
	}
	cms.count += increment
}

// Query returns the estimated count for an item
func (cms *CountMinSketch) Query(item []byte) uint64 {
	var min uint64 = math.MaxUint64
	
	for i := uint(0); i < cms.depth; i++ {
		idx := cms.hash(item, i)
		if cms.table[i][idx] < min {
			min = cms.table[i][idx]
		}
	}
	
	return min
}

// Merge merges another CMS into this one
func (cms *CountMinSketch) Merge(other *CountMinSketch) error {
	return cms.MergeWeighted(other, 1)
}

// MergeWeighted merges other * weight into this CMS
func (cms *CountMinSketch) MergeWeighted(other *CountMinSketch, weight uint64) error {
	if cms.width != other.width || cms.depth != other.depth {
		return ErrCMSDimensionMismatch
	}
	if weight == 0 {
		return nil
	}
	for i := uint(0); i < cms.depth; i++ {
		for j := uint(0); j < cms.width; j++ {
			cms.table[i][j] += other.table[i][j] * weight
		}
	}
	cms.count += other.count * weight
	return nil
}

// Info returns CMS info
func (cms *CountMinSketch) Info() map[string]interface{} {
	return map[string]interface{}{
		"width": cms.width,
		"depth": cms.depth,
		"count": cms.count,
	}
}

// hash computes a hash for the given item and row
func (cms *CountMinSketch) hash(item []byte, row uint) uint {
	h := fnv.New64a()
	h.Write(item)
	h.Write([]byte{byte(row)})
	return uint(h.Sum64() % uint64(cms.width))
}

// ErrCMSDimensionMismatch is returned when CMS dimensions don't match
var ErrCMSDimensionMismatch = fmt.Errorf("CMS dimension mismatch")

type cmsWire struct {
	Width uint       `json:"w"`
	Depth uint       `json:"d"`
	Count uint64     `json:"c"`
	Table [][]uint64 `json:"t"`
}

// EncodeJSON serializes the sketch for Godis opaque DUMP/RESTORE.
func (cms *CountMinSketch) EncodeJSON() ([]byte, error) {
	return json.Marshal(cmsWire{
		Width: cms.width,
		Depth: cms.depth,
		Count: cms.count,
		Table: cms.table,
	})
}

// DecodeCountMinSketch restores a sketch from EncodeJSON output.
func DecodeCountMinSketch(data []byte) (*CountMinSketch, error) {
	var w cmsWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.Width == 0 || w.Depth == 0 || uint(len(w.Table)) != w.Depth {
		return nil, fmt.Errorf("invalid cms data")
	}
	for _, row := range w.Table {
		if uint(len(row)) != w.Width {
			return nil, fmt.Errorf("invalid cms data")
		}
	}
	return &CountMinSketch{
		table: w.Table,
		width: w.Width,
		depth: w.Depth,
		count: w.Count,
	}, nil
}
