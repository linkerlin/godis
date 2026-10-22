package utils

// ToCmdLine convert strings to [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// ToCmdLine2 convert commandName and string-type argument to [][]byte
func ToCmdLine2(commandName string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = []byte(s)
	}
	return result
}

// ToCmdLine3 convert commandName and []byte-type argument to CmdLine
func ToCmdLine3(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = s
	}
	return result
}

// Equals check whether the given value is equal
func Equals(a interface{}, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// BytesEquals check whether the given bytes is equal
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

// ConvertRange converts redis inclusive indices [start, end] to Go slice
// bounds, matching Redis string-family semantics (GETRANGE/BITCOUNT/BITPOS):
// negative offsets count back from the end; after the conversion both bounds
// are clamped into range (start below zero becomes 0, a still-negative end
// becomes 0, end past the last index becomes the last index). Returns
// (-1, -1) for an empty range.
func ConvertRange(start int64, end int64, size int64) (int, int) {
	if start < 0 {
		start += size
		if start < 0 {
			start = 0
		}
	} else if start >= size {
		return -1, -1
	}
	if end < 0 {
		end += size
		if end < 0 {
			end = 0
		}
	} else if end >= size {
		end = size - 1
	}
	if start > end {
		return -1, -1
	}
	return int(start), int(end+1)
}

// RemoveDuplicates removes duplicate byte slices from a 2D byte slice
func RemoveDuplicates(input [][]byte) [][]byte {
	uniqueMap := make(map[string]struct{})
	var result [][]byte

	for _, item := range input {
		// Use bytes.Buffer to convert byte slice to string
		key := string(item)
		if _, exists := uniqueMap[key]; !exists {
			uniqueMap[key] = struct{}{}
			result = append(result, item)
		}
	}

	return result
}
