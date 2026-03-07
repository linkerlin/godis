package timeseries

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Sample represents a single time series sample
type Sample struct {
	Timestamp int64
	Value     float64
}

// TimeSeries represents a time series with samples and retention
type TimeSeries struct {
	Key        string
	Samples    []Sample
	Retention  time.Duration // Retention period (0 = unlimited)
	ChunkSize  int           // Number of samples per chunk
	Labels     map[string]string
	
	// Aggregation rules
	DownsampleRules []DownsampleRule
	
	mu         sync.RWMutex
	lastTimestamp int64
}

// DownsampleRule defines a downsample aggregation rule
type DownsampleRule struct {
	TimeBucket time.Duration
	Aggregation AggregationType
	Destination string // Destination key
}

// AggregationType represents the type of aggregation
type AggregationType int

const (
	AvgAggregation AggregationType = iota
	SumAggregation
	MinAggregation
	MaxAggregation
	CountAggregation
	FirstAggregation
	LastAggregation
	StdPAggregation // Population std dev
	StdSAggregation // Sample std dev
	VarPAggregation
	VarSAggregation
	RangeAggregation
)

// NewTimeSeries creates a new time series
func NewTimeSeries(key string, retention time.Duration) *TimeSeries {
	return &TimeSeries{
		Key:       key,
		Samples:   make([]Sample, 0),
		Retention: retention,
		ChunkSize: 256,
		Labels:    make(map[string]string),
	}
}

// Add adds a sample to the time series
// Returns the added timestamp
func (ts *TimeSeries) Add(timestamp int64, value float64) (int64, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	// If timestamp is 0, use current time
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}
	
	// Check if timestamp is in the past (before retention)
	if ts.Retention > 0 {
		cutoff := time.Now().Add(-ts.Retention).UnixMilli()
		if timestamp < cutoff {
			return 0, ErrTimestampTooOld
		}
	}
	
	// Insert sample maintaining sorted order
	// For efficiency, assume mostly in-order insertions
	if len(ts.Samples) == 0 || timestamp >= ts.lastTimestamp {
		// Append at end
		ts.Samples = append(ts.Samples, Sample{Timestamp: timestamp, Value: value})
		ts.lastTimestamp = timestamp
	} else {
		// Binary search for insertion point
		idx := sort.Search(len(ts.Samples), func(i int) bool {
			return ts.Samples[i].Timestamp >= timestamp
		})
		
		// Check for duplicate timestamp
		if idx < len(ts.Samples) && ts.Samples[idx].Timestamp == timestamp {
			// Update existing value
			ts.Samples[idx].Value = value
			return timestamp, nil
		}
		
		// Insert at idx
		ts.Samples = append(ts.Samples, Sample{})
		copy(ts.Samples[idx+1:], ts.Samples[idx:])
		ts.Samples[idx] = Sample{Timestamp: timestamp, Value: value}
	}
	
	// Apply retention policy
	ts.applyRetention()
	
	return timestamp, nil
}

// Get gets a sample at exact timestamp
func (ts *TimeSeries) Get(timestamp int64) (float64, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	idx := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp >= timestamp
	})
	
	if idx < len(ts.Samples) && ts.Samples[idx].Timestamp == timestamp {
		return ts.Samples[idx].Value, true
	}
	
	return 0, false
}

// GetLast gets the last sample
func (ts *TimeSeries) GetLast() (Sample, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	if len(ts.Samples) == 0 {
		return Sample{}, false
	}
	
	return ts.Samples[len(ts.Samples)-1], true
}

// Range gets samples in a time range [from, to]
func (ts *TimeSeries) Range(from, to int64) []Sample {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	if len(ts.Samples) == 0 {
		return nil
	}
	
	// Find start index
	startIdx := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp >= from
	})
	
	// Find end index
	endIdx := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp > to
	})
	
	if startIdx >= len(ts.Samples) || endIdx <= startIdx {
		return nil
	}
	
	// Return copy
	result := make([]Sample, endIdx-startIdx)
	copy(result, ts.Samples[startIdx:endIdx])
	return result
}

// RangeWithAggregation gets samples with aggregation
func (ts *TimeSeries) RangeWithAggregation(from, to int64, bucketSize time.Duration, agg AggregationType) []Sample {
	samples := ts.Range(from, to)
	if len(samples) == 0 {
		return nil
	}
	
	// Group by buckets
	buckets := make(map[int64][]float64)
	
	for _, s := range samples {
		bucket := s.Timestamp / int64(bucketSize.Milliseconds()) * int64(bucketSize.Milliseconds())
		buckets[bucket] = append(buckets[bucket], s.Value)
	}
	
	// Aggregate each bucket
	var result []Sample
	for bucket, values := range buckets {
		value := aggregate(values, agg)
		result = append(result, Sample{Timestamp: bucket, Value: value})
	}
	
	// Sort by timestamp
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})
	
	return result
}

// Del deletes samples in a range
func (ts *TimeSeries) Del(from, to int64) int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	if len(ts.Samples) == 0 {
		return 0
	}
	
	// Find range to delete
	startIdx := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp >= from
	})
	
	endIdx := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp > to
	})
	
	if startIdx >= len(ts.Samples) || endIdx <= startIdx {
		return 0
	}
	
	count := endIdx - startIdx
	
	// Remove samples
	ts.Samples = append(ts.Samples[:startIdx], ts.Samples[endIdx:]...)
	
	// Update lastTimestamp
	if len(ts.Samples) > 0 {
		ts.lastTimestamp = ts.Samples[len(ts.Samples)-1].Timestamp
	} else {
		ts.lastTimestamp = 0
	}
	
	return count
}

// Trim removes old samples based on retention policy
func (ts *TimeSeries) Trim() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	return ts.applyRetention()
}

func (ts *TimeSeries) applyRetention() int {
	if ts.Retention <= 0 {
		return 0
	}
	
	cutoff := time.Now().Add(-ts.Retention).UnixMilli()
	
	// Find first sample to keep
	idx := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp >= cutoff
	})
	
	if idx == 0 {
		return 0
	}
	
	// Remove old samples
	deleted := idx
	ts.Samples = ts.Samples[idx:]
	
	return deleted
}

// Len returns the number of samples
func (ts *TimeSeries) Len() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	return len(ts.Samples)
}

// Info returns time series info
func (ts *TimeSeries) Info() map[string]interface{} {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	var firstTimestamp, lastTimestamp int64
	if len(ts.Samples) > 0 {
		firstTimestamp = ts.Samples[0].Timestamp
		lastTimestamp = ts.Samples[len(ts.Samples)-1].Timestamp
	}
	
	return map[string]interface{}{
		"totalSamples":   len(ts.Samples),
		"firstTimestamp": firstTimestamp,
		"lastTimestamp":  lastTimestamp,
		"retention":      ts.Retention.Milliseconds(),
		"chunkCount":     (len(ts.Samples) + ts.ChunkSize - 1) / ts.ChunkSize,
	}
}

// AddLabel adds a label
func (ts *TimeSeries) AddLabel(key, value string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	ts.Labels[key] = value
}

// GetLabels returns all labels
func (ts *TimeSeries) GetLabels() map[string]string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	result := make(map[string]string)
	for k, v := range ts.Labels {
		result[k] = v
	}
	return result
}

// aggregate performs aggregation on a set of values
func aggregate(values []float64, agg AggregationType) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	
	switch agg {
	case AvgAggregation:
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
		
	case SumAggregation:
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum
		
	case CountAggregation:
		return float64(len(values))
		
	case MinAggregation:
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min
		
	case MaxAggregation:
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max
		
	case FirstAggregation:
		return values[0]
		
	case LastAggregation:
		return values[len(values)-1]
		
	case RangeAggregation:
		min, max := values[0], values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		return max - min
		
	case StdPAggregation, StdSAggregation:
		avg := aggregate(values, AvgAggregation)
		var sum float64
		for _, v := range values {
			diff := v - avg
			sum += diff * diff
		}
		divisor := float64(len(values))
		if agg == StdSAggregation {
			divisor = float64(len(values) - 1)
		}
		if divisor <= 0 {
			return 0
		}
		return math.Sqrt(sum / divisor)
		
	case VarPAggregation, VarSAggregation:
		avg := aggregate(values, AvgAggregation)
		var sum float64
		for _, v := range values {
			diff := v - avg
			sum += diff * diff
		}
		divisor := float64(len(values))
		if agg == VarSAggregation {
			divisor = float64(len(values) - 1)
		}
		if divisor <= 0 {
			return 0
		}
		return sum / divisor
		
	default:
		return values[0]
	}
}

// AggregationTypeToString converts aggregation type to string
func AggregationTypeToString(agg AggregationType) string {
	switch agg {
	case AvgAggregation:
		return "avg"
	case SumAggregation:
		return "sum"
	case MinAggregation:
		return "min"
	case MaxAggregation:
		return "max"
	case CountAggregation:
		return "count"
	case FirstAggregation:
		return "first"
	case LastAggregation:
		return "last"
	case StdPAggregation:
		return "std.p"
	case StdSAggregation:
		return "std.s"
	case VarPAggregation:
		return "var.p"
	case VarSAggregation:
		return "var.s"
	case RangeAggregation:
		return "range"
	default:
		return ""
	}
}

// ParseAggregationType parses aggregation type string
func ParseAggregationType(s string) (AggregationType, error) {
	switch s {
	case "avg":
		return AvgAggregation, nil
	case "sum":
		return SumAggregation, nil
	case "min":
		return MinAggregation, nil
	case "max":
		return MaxAggregation, nil
	case "count":
		return CountAggregation, nil
	case "first":
		return FirstAggregation, nil
	case "last":
		return LastAggregation, nil
	case "std.p":
		return StdPAggregation, nil
	case "std.s":
		return StdSAggregation, nil
	case "var.p":
		return VarPAggregation, nil
	case "var.s":
		return VarSAggregation, nil
	case "range":
		return RangeAggregation, nil
	default:
		return 0, ErrUnknownAggregation
	}
}

// Errors
var (
	ErrTimestampTooOld   = fmt.Errorf("timestamp is older than retention")
	ErrUnknownAggregation = fmt.Errorf("unknown aggregation type")
)
