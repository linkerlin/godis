// Package validate provides input validation utilities
package validate

import (
	"github.com/hdt3213/godis/lib/consts"
	"github.com/hdt3213/godis/lib/errs"
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
