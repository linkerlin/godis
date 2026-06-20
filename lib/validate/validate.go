// Package validate provides input validation utilities
package validate

import (
	"github.com/linkerlin/godis/lib/consts"
	"github.com/linkerlin/godis/lib/errs"
)

// ValidateKey checks if key is valid
func ValidateKey(key []byte) error {
	if len(key) == 0 {
		return errs.New(errs.ErrCodeInvalidArgs, "key is empty")
	}
	if len(key) > consts.MaxKeySize {
		return errs.Newf(errs.ErrCodeKeyTooLarge, "key too large: %d > %d", len(key), consts.MaxKeySize)
	}
	return nil
}

// ValidateValue checks if value is valid
func ValidateValue(value []byte) error {
	if len(value) > consts.MaxValueSize {
		return errs.Newf(errs.ErrCodeValueTooLarge, "value too large: %d > %d", len(value), consts.MaxValueSize)
	}
	return nil
}

// ValidateBulkBytes applies the same maximum as string values to arbitrary bulk data
// (hash fields/values, list elements, set members, stream fields, etc.).
func ValidateBulkBytes(b []byte) error {
	return ValidateValue(b)
}

// ValidateAppendResult checks the size after APPEND.
func ValidateAppendResult(currentLen, appendLen int) error {
	if int64(currentLen)+int64(appendLen) > int64(consts.MaxValueSize) {
		return errs.Newf(errs.ErrCodeValueTooLarge, "value too large after append")
	}
	return nil
}

// ValidateSetRangeResult checks the resulting string size after SETRANGE.
func ValidateSetRangeResult(currentLen int, offset int64, valueLen int) error {
	end := offset + int64(valueLen)
	cl := int64(currentLen)
	newLen := cl
	if end > newLen {
		newLen = end
	}
	if newLen > int64(consts.MaxValueSize) {
		return errs.Newf(errs.ErrCodeValueTooLarge, "value too large after setrange")
	}
	return nil
}

// ValidateArgsCount checks if argument count is within limit
func ValidateArgsCount(count int) error {
	if count > consts.MaxArgCount {
		return errs.Newf(errs.ErrCodeInvalidArgs, "too many arguments: %d > %d", count, consts.MaxArgCount)
	}
	return nil
}

// ValidateTTL checks if TTL is valid
func ValidateTTL(ttl int64) error {
	if ttl <= 0 {
		return errs.New(errs.ErrCodeInvalidArgs, "invalid expire time")
	}
	return nil
}
