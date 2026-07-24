package timeseries

import (
	"fmt"
	"math"
	"sort"
	"strings"
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
	Key       string
	Samples   []Sample
	Retention time.Duration // Retention period (0 = unlimited)
	ChunkSize int           // Number of samples per chunk
	Labels    map[string]string

	// DuplicatePolicy controls same-timestamp inserts (Redis TS DUPLICATE_POLICY).
	// Default is Block.
	DuplicatePolicy DuplicatePolicy

	// Aggregation rules
	DownsampleRules []DownsampleRule

	mu            sync.RWMutex
	lastTimestamp int64
}

// DuplicatePolicy is RedisTimeSeries duplicate timestamp policy.
type DuplicatePolicy int

const (
	DupBlock DuplicatePolicy = iota // reject duplicate timestamps
	DupFirst                        // keep existing
	DupLast                         // overwrite with new
	DupMin
	DupMax
	DupSum
)

// ParseDuplicatePolicy parses BLOCK/FIRST/LAST/MIN/MAX/SUM.
func ParseDuplicatePolicy(s string) (DuplicatePolicy, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "BLOCK":
		return DupBlock, nil
	case "FIRST":
		return DupFirst, nil
	case "LAST":
		return DupLast, nil
	case "MIN":
		return DupMin, nil
	case "MAX":
		return DupMax, nil
	case "SUM":
		return DupSum, nil
	default:
		return DupBlock, fmt.Errorf("unknown duplicate policy")
	}
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
	TwaAggregation // time-weighted average
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

// Add adds a sample using the series DuplicatePolicy.
func (ts *TimeSeries) Add(timestamp int64, value float64) (int64, error) {
	return ts.AddWithPolicy(timestamp, value, ts.DuplicatePolicy)
}

// AddWithPolicy adds a sample with an explicit duplicate policy (ON_DUPLICATE override).
func (ts *TimeSeries) AddWithPolicy(timestamp int64, value float64, policy DuplicatePolicy) (int64, error) {
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
	if len(ts.Samples) == 0 || timestamp > ts.lastTimestamp {
		ts.Samples = append(ts.Samples, Sample{Timestamp: timestamp, Value: value})
		ts.lastTimestamp = timestamp
	} else if timestamp == ts.lastTimestamp {
		if err := applyDuplicatePolicy(&ts.Samples[len(ts.Samples)-1], value, policy); err != nil {
			return 0, err
		}
	} else {
		idx := sort.Search(len(ts.Samples), func(i int) bool {
			return ts.Samples[i].Timestamp >= timestamp
		})

		if idx < len(ts.Samples) && ts.Samples[idx].Timestamp == timestamp {
			if err := applyDuplicatePolicy(&ts.Samples[idx], value, policy); err != nil {
				return 0, err
			}
			return timestamp, nil
		}

		ts.Samples = append(ts.Samples, Sample{})
		copy(ts.Samples[idx+1:], ts.Samples[idx:])
		ts.Samples[idx] = Sample{Timestamp: timestamp, Value: value}
	}

	ts.applyRetention()
	return timestamp, nil
}

func applyDuplicatePolicy(sample *Sample, newVal float64, policy DuplicatePolicy) error {
	switch policy {
	case DupBlock:
		return ErrDuplicateTimestamp
	case DupFirst:
		return nil
	case DupLast:
		sample.Value = newVal
	case DupMin:
		if newVal < sample.Value {
			sample.Value = newVal
		}
	case DupMax:
		if newVal > sample.Value {
			sample.Value = newVal
		}
	case DupSum:
		sample.Value += newVal
	default:
		return ErrDuplicateTimestamp
	}
	return nil
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

	bucketMs := int64(bucketSize.Milliseconds())
	if bucketMs <= 0 {
		return nil
	}

	// Group by buckets
	buckets := make(map[int64][]Sample)
	for _, s := range samples {
		bucket := s.Timestamp / bucketMs * bucketMs
		buckets[bucket] = append(buckets[bucket], s)
	}

	// Aggregate each bucket
	var result []Sample
	for bucket, bucketSamples := range buckets {
		bucketEnd := bucket + bucketMs
		if bucketEnd > to+1 {
			bucketEnd = to + 1
		}
		var value float64
		if agg == TwaAggregation {
			value = aggregateTWA(bucketSamples, bucket, bucketEnd)
		} else {
			values := make([]float64, len(bucketSamples))
			for i, s := range bucketSamples {
				values[i] = s.Value
			}
			value = aggregate(values, agg)
		}
		result = append(result, Sample{Timestamp: bucket, Value: value})
	}

	// Sort by timestamp
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

// aggregateTWA computes time-weighted average within [bucketStart, bucketEnd).
func aggregateTWA(samples []Sample, bucketStart, bucketEnd int64) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	var weightedSum, totalDuration float64
	for i, s := range samples {
		start := s.Timestamp
		if start < bucketStart {
			start = bucketStart
		}
		end := bucketEnd
		if i+1 < len(samples) && samples[i+1].Timestamp < end {
			end = samples[i+1].Timestamp
		}
		if end <= start {
			continue
		}
		dur := float64(end - start)
		weightedSum += s.Value * dur
		totalDuration += dur
	}
	if totalDuration == 0 {
		return samples[len(samples)-1].Value
	}
	return weightedSum / totalDuration
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
	case TwaAggregation:
		return "twa"
	default:
		return ""
	}
}

// ParseAggregationType parses aggregation type string
func ParseAggregationType(s string) (AggregationType, error) {
	switch strings.ToLower(s) {
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
	case "twa":
		return TwaAggregation, nil
	default:
		return 0, ErrUnknownAggregation
	}
}

// Errors
var (
	ErrTimestampTooOld     = fmt.Errorf("timestamp is older than retention")
	ErrUnknownAggregation  = fmt.Errorf("unknown aggregation type")
	ErrDuplicateTimestamp  = fmt.Errorf("duplicate timestamp")
)


// SetRetention sets the retention period
func (ts *TimeSeries) SetRetention(retention time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	ts.Retention = retention
	ts.applyRetention()
}

// GetRetention returns the retention period
func (ts *TimeSeries) GetRetention() time.Duration {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	return ts.Retention
}

// SetLabels sets all labels (replaces existing labels)
func (ts *TimeSeries) SetLabels(labels map[string]string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	ts.Labels = labels
}

// AddDownsampleRule adds a downsample rule
func (ts *TimeSeries) AddDownsampleRule(rule DownsampleRule) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	// Check if rule for this destination already exists
	for i, r := range ts.DownsampleRules {
		if r.Destination == rule.Destination {
			ts.DownsampleRules[i] = rule
			return
		}
	}
	
	ts.DownsampleRules = append(ts.DownsampleRules, rule)
}

// RemoveDownsampleRule removes a downsample rule by destination key
func (ts *TimeSeries) RemoveDownsampleRule(destKey string) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	
	for i, r := range ts.DownsampleRules {
		if r.Destination == destKey {
			ts.DownsampleRules = append(ts.DownsampleRules[:i], ts.DownsampleRules[i+1:]...)
			return true
		}
	}
	
	return false
}

// GetDownsampleRules returns all downsample rules
func (ts *TimeSeries) GetDownsampleRules() []DownsampleRule {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	
	result := make([]DownsampleRule, len(ts.DownsampleRules))
	copy(result, ts.DownsampleRules)
	return result
}
