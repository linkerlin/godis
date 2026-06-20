package database

import (
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/errs"
	"github.com/linkerlin/godis/lib/validate"
	"github.com/linkerlin/godis/redis/protocol"
)

func validateCmdArgCount(cmdLine [][]byte) redis.Reply {
	if err := validate.ValidateArgsCount(len(cmdLine)); err != nil {
		return protocol.MakeErrReply("ERR too many arguments")
	}
	return nil
}

func validatePreparedKeyStrings(write, read []string) redis.Reply {
	for _, k := range write {
		if reply := validateKeyString(k); reply != nil {
			return reply
		}
	}
	for _, k := range read {
		if reply := validateKeyString(k); reply != nil {
			return reply
		}
	}
	return nil
}

func validateKeyString(key string) redis.Reply {
	if err := validate.ValidateKey([]byte(key)); err != nil {
		if errs.Is(err, errs.ErrCodeKeyTooLarge) {
			return protocol.MakeErrReply("ERR key too large")
		}
		return protocol.MakeErrReply("ERR invalid key")
	}
	return nil
}

func validateKeyBytes(key []byte) redis.Reply {
	if err := validate.ValidateKey(key); err != nil {
		if errs.Is(err, errs.ErrCodeKeyTooLarge) {
			return protocol.MakeErrReply("ERR key too large")
		}
		return protocol.MakeErrReply("ERR invalid key")
	}
	return nil
}

func validateStreamKeyNames(keys []string) redis.Reply {
	for _, k := range keys {
		if reply := validateKeyString(k); reply != nil {
			return reply
		}
	}
	return nil
}

func validateBulkBytes(b []byte) redis.Reply {
	if err := validate.ValidateBulkBytes(b); err != nil {
		return protocol.MakeErrReply("ERR value too large")
	}
	return nil
}

func validateBulkBytesSlice(bs [][]byte) redis.Reply {
	for _, b := range bs {
		if reply := validateBulkBytes(b); reply != nil {
			return reply
		}
	}
	return nil
}

func validateAppendGrowth(currentLen, addLen int) redis.Reply {
	if err := validate.ValidateAppendResult(currentLen, addLen); err != nil {
		return protocol.MakeErrReply("ERR value too large")
	}
	return nil
}

func validateSetRangeGrowth(currentLen int, offset int64, valueLen int) redis.Reply {
	if err := validate.ValidateSetRangeResult(currentLen, offset, valueLen); err != nil {
		return protocol.MakeErrReply("ERR value too large")
	}
	return nil
}
