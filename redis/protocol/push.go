package protocol

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/hdt3213/godis/interface/redis"
)

// PushMessage represents a RESP3 push message
// Used for client-side caching invalidation, pub/sub, etc.
type PushMessage struct {
	Kind string
	Data []redis.Reply
}

// MakePushMessage creates a new push message
func MakePushMessage(kind string, data []redis.Reply) *PushMessage {
	return &PushMessage{
		Kind: kind,
		Data: data,
	}
}

// ToBytes marshals push message to RESP3 format
// Format: >3\r\n$10\r\ninvalidate\r\n...
func (p *PushMessage) ToBytes() []byte {
	var buf bytes.Buffer
	
	// Push type character and number of elements
	buf.WriteString(fmt.Sprintf(">%d\r\n", len(p.Data)+1))
	
	// Push kind as bulk string
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(p.Kind), p.Kind))
	
	// Push data elements
	for _, reply := range p.Data {
		buf.Write(reply.ToBytes())
	}
	
	return buf.Bytes()
}

// MakeInvalidatePush creates an invalidation push message for client-side caching
// invalidate: []string{"key1", "key2"}
func MakeInvalidatePush(keys []string) *PushMessage {
	var keyReplies []redis.Reply
	
	// Create array of keys
	keyBytes := make([][]byte, len(keys))
	for i, key := range keys {
		keyBytes[i] = []byte(key)
	}
	
	keyReplies = append(keyReplies, MakeMultiBulkReply(keyBytes))
	
	return MakePushMessage("invalidate", keyReplies)
}

// MakeMessagePush creates a pub/sub message push
// message: channel, message
func MakeMessagePush(channel string, message []byte) *PushMessage {
	return MakePushMessage("message", []redis.Reply{
		MakeBulkReply([]byte(channel)),
		MakeBulkReply(message),
	})
}

// MakeSubscribePush creates a subscribe confirmation push
func MakeSubscribePush(channel string, numSub int) *PushMessage {
	return MakePushMessage("subscribe", []redis.Reply{
		MakeBulkReply([]byte(channel)),
		MakeIntReply(int64(numSub)),
	})
}

// MakeUnsubscribePush creates an unsubscribe confirmation push
func MakeUnsubscribePush(channel string, numSub int) *PushMessage {
	return MakePushMessage("unsubscribe", []redis.Reply{
		MakeBulkReply([]byte(channel)),
		MakeIntReply(int64(numSub)),
	})
}

// MakeSMessagePush creates a sharded pub/sub message push
func MakeSMessagePush(channel string, message []byte) *PushMessage {
	return MakePushMessage("smessage", []redis.Reply{
		MakeBulkReply([]byte(channel)),
		MakeBulkReply(message),
	})
}

// PushReplyWriter handles writing push messages to connections
type PushReplyWriter interface {
	WritePush(reply redis.Reply) error
}

// ParsePush parses a RESP3 push message from bytes
func ParsePush(data []byte) (*PushMessage, error) {
	if len(data) == 0 || data[0] != '>' {
		return nil, fmt.Errorf("not a push message")
	}
	
	// Parse number of elements
	endIdx := bytes.Index(data, []byte("\r\n"))
	if endIdx == -1 {
		return nil, fmt.Errorf("invalid push message format")
	}
	
	countStr := string(data[1:endIdx])
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, fmt.Errorf("invalid push count: %v", err)
	}
	
	// Parse elements
	pos := endIdx + 2
	parser := &resp3Parser{data: data[pos:]}
	
	var elements []redis.Reply
	for i := 0; i < count; i++ {
		elem, err := parser.parseNext()
		if err != nil {
			return nil, err
		}
		elements = append(elements, elem)
	}
	
	if len(elements) == 0 {
		return nil, fmt.Errorf("empty push message")
	}
	
	// First element is the kind
	kindReply, ok := elements[0].(*BulkReply)
	if !ok {
		return nil, fmt.Errorf("push kind must be bulk string")
	}
	
	return &PushMessage{
		Kind: string(kindReply.Arg),
		Data: elements[1:],
	}, nil
}

// resp3Parser is a simple RESP3 parser
type resp3Parser struct {
	data []byte
	pos  int
}

func (p *resp3Parser) parseNext() (redis.Reply, error) {
	if p.pos >= len(p.data) {
		return nil, fmt.Errorf("no more data")
	}
	
	ch := p.data[p.pos]
	p.pos++
	
	switch ch {
	case '$':
		return p.parseBulk()
	case ':':
		return p.parseInt()
	case '*':
		return p.parseArray()
	default:
		return nil, fmt.Errorf("unknown type: %c", ch)
	}
}

func (p *resp3Parser) parseBulk() (redis.Reply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	length, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}
	
	if length < 0 {
		return MakeNullBulkReply(), nil
	}
	
	if p.pos+length+2 > len(p.data) {
		return nil, fmt.Errorf("not enough data")
	}
	
	data := p.data[p.pos : p.pos+length]
	p.pos += length + 2 // +2 for \r\n
	
	return MakeBulkReply(data), nil
}

func (p *resp3Parser) parseInt() (redis.Reply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	val, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return nil, err
	}
	
	return MakeIntReply(val), nil
}

func (p *resp3Parser) parseArray() (redis.Reply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	count, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}
	
	if count < 0 {
		return MakeEmptyMultiBulkReply(), nil
	}
	
	var elements [][]byte
	for i := 0; i < count; i++ {
		elem, err := p.parseNext()
		if err != nil {
			return nil, err
		}
		if bulk, ok := elem.(*BulkReply); ok {
			elements = append(elements, bulk.Arg)
		} else {
			elements = append(elements, elem.ToBytes())
		}
	}
	
	return MakeMultiBulkReply(elements), nil
}

func (p *resp3Parser) readLine() ([]byte, error) {
	start := p.pos
	for p.pos < len(p.data) {
		if p.data[p.pos] == '\r' && p.pos+1 < len(p.data) && p.data[p.pos+1] == '\n' {
			line := p.data[start:p.pos]
			p.pos += 2
			return line, nil
		}
		p.pos++
	}
	return nil, fmt.Errorf("line not found")
}
