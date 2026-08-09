package protocol

import "strconv"

// MovedErrReply is Redis Cluster -MOVED <slot> <ip:port>
type MovedErrReply struct {
	Slot uint32
	Addr string
}

// MakeMovedErrReply creates a MOVED redirection error.
func MakeMovedErrReply(slot uint32, addr string) *MovedErrReply {
	return &MovedErrReply{Slot: slot, Addr: addr}
}

func (r *MovedErrReply) Error() string {
	return "MOVED " + strconv.FormatUint(uint64(r.Slot), 10) + " " + r.Addr
}

func (r *MovedErrReply) ToBytes() []byte {
	return []byte("-" + r.Error() + "\r\n")
}

// AskErrReply is Redis Cluster -ASK <slot> <ip:port>
type AskErrReply struct {
	Slot uint32
	Addr string
}

// MakeAskErrReply creates an ASK redirection error.
func MakeAskErrReply(slot uint32, addr string) *AskErrReply {
	return &AskErrReply{Slot: slot, Addr: addr}
}

func (r *AskErrReply) Error() string {
	return "ASK " + strconv.FormatUint(uint64(r.Slot), 10) + " " + r.Addr
}

func (r *AskErrReply) ToBytes() []byte {
	return []byte("-" + r.Error() + "\r\n")
}
