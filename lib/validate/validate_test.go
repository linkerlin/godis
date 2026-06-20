package validate

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/consts"
	"github.com/linkerlin/godis/lib/errs"
)

func TestValidateKey(t *testing.T) {
	if err := ValidateKey([]byte("ok")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateKey(nil); err == nil {
		t.Fatal("expected error for empty key")
	} else if !errs.Is(err, errs.ErrCodeInvalidArgs) {
		t.Fatalf("expected ErrCodeInvalidArgs, got %v", err)
	}
	oversized := make([]byte, consts.MaxKeySize+1)
	if err := ValidateKey(oversized); err == nil {
		t.Fatal("expected error for oversized key")
	} else if !strings.Contains(err.Error(), "key too large") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateValue(t *testing.T) {
	if err := ValidateValue([]byte("value")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateValue(nil); err != nil {
		t.Fatalf("nil value should be allowed: %v", err)
	}
	oversized := make([]byte, consts.MaxValueSize+1)
	if err := ValidateValue(oversized); err == nil {
		t.Fatal("expected error for oversized value")
	} else if !strings.Contains(err.Error(), "value too large") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateBulkBytes(t *testing.T) {
	if err := ValidateBulkBytes([]byte("bulk")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateAppendResult(t *testing.T) {
	if err := ValidateAppendResult(10, 20); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateAppendResult(consts.MaxValueSize, 1); err == nil {
		t.Fatal("expected error when append exceeds max value size")
	} else if !strings.Contains(err.Error(), "value too large after append") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSetRangeResult(t *testing.T) {
	if err := ValidateSetRangeResult(5, 0, 3); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateSetRangeResult(0, 0, consts.MaxValueSize); err != nil {
		t.Fatalf("exact max should be allowed: %v", err)
	}
	if err := ValidateSetRangeResult(0, 0, consts.MaxValueSize+1); err == nil {
		t.Fatal("expected error when setrange exceeds max value size")
	}
}

func TestValidateArgsCount(t *testing.T) {
	if err := ValidateArgsCount(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateArgsCount(consts.MaxArgCount + 1); err == nil {
		t.Fatal("expected error for too many args")
	} else if !strings.Contains(err.Error(), "too many arguments") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateArgsCount(consts.MaxArgCount); err != nil {
		t.Fatalf("max arg count should be allowed: %v", err)
	}
}

func TestValidateTTL(t *testing.T) {
	if err := ValidateTTL(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, ttl := range []int64{0, -1} {
		if err := ValidateTTL(ttl); err == nil {
			t.Fatalf("expected error for ttl=%d", ttl)
		} else if !errs.Is(err, errs.ErrCodeInvalidArgs) {
			t.Fatalf("expected ErrCodeInvalidArgs, got %v", err)
		}
	}
}

func TestValidateErrorMessages(t *testing.T) {
	err := ValidateKey(make([]byte, consts.MaxKeySize+1))
	if err == nil || !strings.Contains(err.Error(), "key too large") {
		t.Fatalf("expected descriptive key error, got %v", err)
	}
}
