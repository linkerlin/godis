package errs

import (
	"errors"
	"strings"
	"testing"
)

func TestNewAndNewf(t *testing.T) {
	err := New(ErrCodeInvalidArgs, "bad argument")
	if err == nil || err.Error() != "bad argument" || !Is(err, ErrCodeInvalidArgs) {
		t.Fatalf("New: %v", err)
	}
	err = Newf(ErrCodeInternal, "value %d", 7)
	if err == nil || err.Error() != "value 7" || !Is(err, ErrCodeInternal) {
		t.Fatalf("Newf: %v", err)
	}
	if Is(err, ErrCodeInvalidArgs) {
		t.Fatal("Is matched the wrong code")
	}
	if Is(errors.New("plain"), ErrCodeInternal) {
		t.Fatal("plain error should not match a Godis code")
	}
}

func TestWrapAndCause(t *testing.T) {
	if Wrap(nil, "ctx") != nil || Wrapf(nil, "ctx %d", 1) != nil {
		t.Fatal("wrapping nil should return nil")
	}
	root := errors.New("root")
	wrapped := Wrap(root, "context")
	if wrapped == nil || !strings.Contains(wrapped.Error(), "context") || !strings.Contains(wrapped.Error(), "root") {
		t.Fatalf("Wrap: %v", wrapped)
	}
	if Cause(wrapped) != root {
		t.Fatalf("Cause: %v", Cause(wrapped))
	}
	wrapped = Wrapf(root, "context %d", 2)
	if wrapped == nil || !strings.Contains(wrapped.Error(), "context 2") {
		t.Fatalf("Wrapf: %v", wrapped)
	}
}

func TestPredicatesAndFormatting(t *testing.T) {
	wrong := New(ErrCodeWrongType, "wrong type")
	if !IsWrongType(wrong) || IsKeyNotFound(wrong) || IsDBIndexOutOfRange(wrong) {
		t.Fatal("wrong predicate result")
	}
	notFound := New(ErrCodeKeyNotFound, "missing")
	if !IsKeyNotFound(notFound) {
		t.Fatal("IsKeyNotFound")
	}
	badDB := New(ErrCodeDBIndexOutOfRange, "db")
	if !IsDBIndexOutOfRange(badDB) {
		t.Fatal("IsDBIndexOutOfRange")
	}
	if got := Format(wrong); got != "wrong type" {
		t.Fatalf("Format=%q", got)
	}
	if got := FormatStack(wrong); !strings.Contains(got, "wrong type") || !strings.Contains(got, "errors_test.go") {
		t.Fatalf("FormatStack missing stack: %q", got)
	}
	codeErr := &GodisError{Code: ErrCodeIO, Message: "io", Cause: errors.New("disk")}
	if codeErr.Error() != "io: disk" {
		t.Fatalf("GodisError.Error=%q", codeErr.Error())
	}
}
