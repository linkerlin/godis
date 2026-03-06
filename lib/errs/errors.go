// Package errs provides unified error handling with stack traces
package errs

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// ErrorCode defines error types
type ErrorCode int

const (
	ErrCodeUnknown ErrorCode = iota
	ErrCodeWrongType
	ErrCodeProtocol
	ErrCodeKeyNotFound
	ErrCodeInvalidArgs
	ErrCodeInternal
	ErrCodeDBIndexOutOfRange
	ErrCodeKeyTooLarge
	ErrCodeValueTooLarge
	ErrCodeSyntax
	ErrCodePermission
	ErrCodeIO
)

// GodisError is the unified error type for godis
type GodisError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *GodisError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// New creates a new error with stack trace
func New(code ErrorCode, msg string) error {
	return errors.WithStack(&GodisError{
		Code:    code,
		Message: msg,
	})
}

// Newf creates a new formatted error with stack trace
func Newf(code ErrorCode, format string, args ...interface{}) error {
	return errors.NewWithDepthf(1, format, args...)
}

// Wrap wraps an error with additional context and stack trace
func Wrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	return errors.Wrap(err, msg)
}

// Wrapf wraps an error with formatted message
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return errors.Wrapf(err, format, args...)
}

// Is checks if an error is of a specific code
func Is(err error, code ErrorCode) bool {
	var ge *GodisError
	if errors.As(err, &ge) {
		return ge.Code == code
	}
	return false
}

// IsWrongType checks if error is wrong type error
func IsWrongType(err error) bool {
	return Is(err, ErrCodeWrongType)
}

// IsKeyNotFound checks if error is key not found
func IsKeyNotFound(err error) bool {
	return Is(err, ErrCodeKeyNotFound)
}

// IsDBIndexOutOfRange checks if error is db index out of range
func IsDBIndexOutOfRange(err error) bool {
	return Is(err, ErrCodeDBIndexOutOfRange)
}

// Cause returns the root cause of an error
func Cause(err error) error {
	return errors.UnwrapAll(err)
}

// FormatStack returns full error info with stack trace
func FormatStack(err error) string {
	return fmt.Sprintf("%+v", err)
}

// Format returns error message
func Format(err error) string {
	return fmt.Sprintf("%v", err)
}
