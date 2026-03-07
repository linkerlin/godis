package vector

import (
	"math"
	"sync"
)

// Vector represents a high-dimensional vector with float32 components
// This is optimized for AI/ML embeddings where float32 provides sufficient precision
type Vector struct {
	Data   []float32
	Dim    int
	mu     sync.RWMutex
}

// NewVector creates a new vector from float32 slice
func NewVector(data []float32) *Vector {
	v := &Vector{
		Data: make([]float32, len(data)),
		Dim:  len(data),
	}
	copy(v.Data, data)
	return v
}

// NewVectorFromFloat64 creates a vector from float64 slice (converted to float32)
func NewVectorFromFloat64(data []float64) *Vector {
	v := &Vector{
		Data: make([]float32, len(data)),
		Dim:  len(data),
	}
	for i, f := range data {
		v.Data[i] = float32(f)
	}
	return v
}

// Clone creates a deep copy of the vector
func (v *Vector) Clone() *Vector {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	return NewVector(v.Data)
}

// Normalize normalizes the vector to unit length (L2 norm)
func (v *Vector) Normalize() {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	norm := float32(0)
	for _, x := range v.Data {
		norm += x * x
	}
	norm = float32(math.Sqrt(float64(norm)))
	
	if norm > 0 {
		for i := range v.Data {
			v.Data[i] /= norm
		}
	}
}

// DotProduct computes dot product with another vector
func (v *Vector) DotProduct(other *Vector) float32 {
	v.mu.RLock()
	other.mu.RLock()
	defer v.mu.RUnlock()
	defer other.mu.RUnlock()
	
	if v.Dim != other.Dim {
		return 0
	}
	
	var result float32
	for i := 0; i < v.Dim; i++ {
		result += v.Data[i] * other.Data[i]
	}
	return result
}

// CosineSimilarity computes cosine similarity with another vector
// Returns value in range [-1, 1], where 1 means identical direction
func (v *Vector) CosineSimilarity(other *Vector) float32 {
	v.mu.RLock()
	other.mu.RLock()
	defer v.mu.RUnlock()
	defer other.mu.RUnlock()
	
	if v.Dim != other.Dim {
		return 0
	}
	
	var dotProduct, normV, normO float32
	for i := 0; i < v.Dim; i++ {
		dotProduct += v.Data[i] * other.Data[i]
		normV += v.Data[i] * v.Data[i]
		normO += other.Data[i] * other.Data[i]
	}
	
	if normV == 0 || normO == 0 {
		return 0
	}
	
	return dotProduct / (float32(math.Sqrt(float64(normV))) * float32(math.Sqrt(float64(normO))))
}

// EuclideanDistance computes L2 distance to another vector
func (v *Vector) EuclideanDistance(other *Vector) float32 {
	v.mu.RLock()
	other.mu.RLock()
	defer v.mu.RUnlock()
	defer other.mu.RUnlock()
	
	if v.Dim != other.Dim {
		return math.MaxFloat32
	}
	
	var sum float32
	for i := 0; i < v.Dim; i++ {
		diff := v.Data[i] - other.Data[i]
		sum += diff * diff
	}
	
	return float32(math.Sqrt(float64(sum)))
}

// ToFloat64 converts vector to float64 slice
func (v *Vector) ToFloat64() []float64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	result := make([]float64, v.Dim)
	for i, f := range v.Data {
		result[i] = float64(f)
	}
	return result
}

// Equals checks if two vectors are equal
func (v *Vector) Equals(other *Vector) bool {
	v.mu.RLock()
	other.mu.RLock()
	defer v.mu.RUnlock()
	defer other.mu.RUnlock()
	
	if v.Dim != other.Dim {
		return false
	}
	
	for i := 0; i < v.Dim; i++ {
		if v.Data[i] != other.Data[i] {
			return false
		}
	}
	return true
}

// Dimension returns the vector dimension
func (v *Vector) Dimension() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.Dim
}

// Magnitude returns the L2 norm (magnitude) of the vector
func (v *Vector) Magnitude() float32 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	var sum float32
	for _, x := range v.Data {
		sum += x * x
	}
	return float32(math.Sqrt(float64(sum)))
}
