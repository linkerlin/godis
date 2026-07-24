package geohash

import (
	"encoding/base32"
	"encoding/binary"
)

var bits = []uint8{128, 64, 32, 16, 8, 4, 2, 1}
var enc = base32.NewEncoding("0123456789bcdefghjkmnpqrstuvwxyz").WithPadding(base32.NoPadding)

// defaultBitSize is 52 to match Redis GEO: the score is stored as float64 and
// must fit in the 53-bit mantissa without precision loss (G-2).
const defaultBitSize = 52

// return: geohash, box
func encode0(latitude, longitude float64, bitSize uint) ([]byte, [2][2]float64) {
	box := [2][2]float64{
		{-180, 180}, // lng
		{-90, 90},   // lat
	}
	pos := [2]float64{longitude, latitude}
	hashLen := bitSize >> 3
	if bitSize&7 > 0 {
		hashLen++
	}
	hash := make([]byte, hashLen)
	var precision uint = 0
	for precision < bitSize {
		for direction, val := range pos {
			mid := (box[direction][0] + box[direction][1]) / 2
			if val < mid {
				box[direction][1] = mid
			} else {
				box[direction][0] = mid
				hash[precision>>3] |= 1 << (7 - precision&7)
			}
			precision++
			if precision == bitSize {
				break
			}
		}
	}

	return hash, box
}

// Encode converts latitude and longitude to a 52-bit geohash integer
// (low 52 bits). Safe to store as float64 sorted-set score without rounding.
func Encode(latitude, longitude float64) uint64 {
	buf, _ := encode0(latitude, longitude, defaultBitSize)
	return bytesToHash52(buf)
}

func decode0(hash []byte, bitSize uint) [][]float64 {
	box := [][]float64{
		{-180, 180},
		{-90, 90},
	}
	direction := 0
	var precision uint
	for i := 0; i < len(hash) && precision < bitSize; i++ {
		code := hash[i]
		for j := 0; j < len(bits) && precision < bitSize; j++ {
			mid := (box[direction][0] + box[direction][1]) / 2
			mask := bits[j]
			if mask&code > 0 {
				box[direction][0] = mid
			} else {
				box[direction][1] = mid
			}
			direction = (direction + 1) % 2
			precision++
		}
	}
	return box
}

// Decode converts a 52-bit geohash code to latitude and longitude
func Decode(code uint64) (float64, float64) {
	buf := FromInt(code)
	box := decode0(buf, defaultBitSize)
	lng := float64(box[0][0]+box[0][1]) / 2
	lat := float64(box[1][0]+box[1][1]) / 2
	return lat, lng
}

// ToString converts bytes geohash code to base32 string
func ToString(buf []byte) string {
	return enc.EncodeToString(buf)
}

// bytesToHash52 packs encode0 output (52 bits MSB-first) into a uint64.
func bytesToHash52(buf []byte) uint64 {
	var code uint64
	for _, b := range buf {
		code = (code << 8) | uint64(b)
	}
	// encode0 with 52 bits uses 7 bytes (56 bit slots); low 4 bits unused
	if len(buf) >= 7 {
		return code >> 4
	}
	return code
}

// ToInt converts bytes geohash code to uint64 code (52-bit when from Encode)
func ToInt(buf []byte) uint64 {
	if len(buf) == 0 {
		return 0
	}
	// Full 8-byte legacy / neighbour prefixes: treat as big-endian uint64
	if len(buf) == 8 {
		return binary.BigEndian.Uint64(buf)
	}
	return bytesToHash52(buf)
}

// FromInt converts a 52-bit geohash code to the byte form used by ToString.
func FromInt(code uint64) []byte {
	shifted := code << 4
	buf := make([]byte, 7)
	for i := 6; i >= 0; i-- {
		buf[i] = byte(shifted)
		shifted >>= 8
	}
	return buf
}
