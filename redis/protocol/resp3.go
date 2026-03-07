package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/hdt3213/godis/interface/redis"
)

// RESP3 Type Characters
const (
	// Simple types
	Resp3SimpleString = '+'  // +\r\n
	Resp3SimpleError  = '-'  // -\r\n
	Resp3Integer      = ':'  // :\r\n
	Resp3Null         = '_'  // _\r\n
	Resp3Double       = ','  // ,\r\n
	Resp3Boolean      = '#'  // #t\r\n or #f\r\n
	Resp3BigNumber    = '('  // (\r\n

	// Aggregate types
	Resp3BulkString  = '$'  // $\r\n
	Resp3Array       = '*'  // *\r\n
	Resp3Map         = '%'  // %\r\n
	Resp3Set         = '~'  // ~\r\n
	Resp3Attribute   = '|'  // |\r\n
	Resp3Push        = '>'  // >\r\n
	Resp3Verbatim    = '='  // =\r\n

	// Streamed types
	Resp3StreamedString = '$' // with ? as length
)

// RESP3Reply is the interface for RESP3 replies
type RESP3Reply interface {
	redis.Reply
	ToRESP3() []byte
}

// NullReply represents RESP3 null value
type NullReply struct{}

// ToBytes marshals redis.Reply
func (r *NullReply) ToBytes() []byte {
	return []byte("_\r\n")
}

// DoubleReply represents a double precision float
type DoubleReply struct {
	Value float64
}

// MakeDoubleReply creates a new DoubleReply
func MakeDoubleReply(val float64) *DoubleReply {
	return &DoubleReply{Value: val}
}

// ToBytes marshals redis.Reply
func (r *DoubleReply) ToBytes() []byte {
	return []byte(fmt.Sprintf(",%s\r\n", formatFloat(r.Value)))
}

// BooleanReply represents a boolean value
type BooleanReply struct {
	Value bool
}

// MakeBooleanReply creates a new BooleanReply
func MakeBooleanReply(val bool) *BooleanReply {
	return &BooleanReply{Value: val}
}

// ToBytes marshals redis.Reply
func (r *BooleanReply) ToBytes() []byte {
	if r.Value {
		return []byte("#t\r\n")
	}
	return []byte("#f\r\n")
}

// BigNumberReply represents an arbitrary precision number
type BigNumberReply struct {
	Value string // Big numbers are sent as strings
}

// MakeBigNumberReply creates a new BigNumberReply
func MakeBigNumberReply(val string) *BigNumberReply {
	return &BigNumberReply{Value: val}
}

// ToBytes marshals redis.Reply
func (r *BigNumberReply) ToBytes() []byte {
	return []byte(fmt.Sprintf("(%s\r\n", r.Value))
}

// VerbatimReply represents a verbatim string with format
type VerbatimReply struct {
	Format string // txt, mkd, etc.
	Value  string
}

// MakeVerbatimReply creates a new VerbatimReply
func MakeVerbatimReply(format, value string) *VerbatimReply {
	return &VerbatimReply{Format: format, Value: value}
}

// ToBytes marshals redis.Reply
func (r *VerbatimReply) ToBytes() []byte {
	content := fmt.Sprintf("%s:%s", r.Format, r.Value)
	return []byte(fmt.Sprintf("=%d\r\n%s\r\n", len(content), content))
}

// MapReply represents a map of key-value pairs
type MapReply struct {
	Data map[string]redis.Reply
}

// MakeMapReply creates a new MapReply
func MakeMapReply() *MapReply {
	return &MapReply{Data: make(map[string]redis.Reply)}
}

// Put adds a key-value pair to the map
func (r *MapReply) Put(key string, value redis.Reply) {
	r.Data[key] = value
}

// ToBytes marshals redis.Reply
func (r *MapReply) ToBytes() []byte {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%%%d\r\n", len(r.Data)))
	for k, v := range r.Data {
		buf.Write(MakeBulkReply([]byte(k)).ToBytes())
		buf.Write(v.ToBytes())
	}
	return buf.Bytes()
}

// SetReply represents an unordered set of elements
type SetReply struct {
	Data []redis.Reply
}

// MakeSetReply creates a new SetReply
func MakeSetReply(data []redis.Reply) *SetReply {
	return &SetReply{Data: data}
}

// ToBytes marshals redis.Reply
func (r *SetReply) ToBytes() []byte {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("~%d\r\n", len(r.Data)))
	for _, elem := range r.Data {
		buf.Write(elem.ToBytes())
	}
	return buf.Bytes()
}

// AttributeReply wraps another reply with attributes
type AttributeReply struct {
	Attributes *MapReply
	Reply      redis.Reply
}

// MakeAttributeReply creates a new AttributeReply
func MakeAttributeReply(attrs *MapReply, reply redis.Reply) *AttributeReply {
	return &AttributeReply{Attributes: attrs, Reply: reply}
}

// ToBytes marshals redis.Reply
func (r *AttributeReply) ToBytes() []byte {
	var buf bytes.Buffer
	buf.Write(r.Attributes.ToBytes())
	buf.Write(r.Reply.ToBytes())
	return buf.Bytes()
}

// PushReply represents a push message (for client-side caching, pub/sub, etc.)
type PushReply struct {
	Kind    string
	Data    []redis.Reply
}

// MakePushReply creates a new PushReply
func MakePushReply(kind string, data []redis.Reply) *PushReply {
	return &PushReply{Kind: kind, Data: data}
}

// ToBytes marshals redis.Reply
func (r *PushReply) ToBytes() []byte {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(">%d\r\n", len(r.Data)+1))
	buf.Write(MakeBulkReply([]byte(r.Kind)).ToBytes())
	for _, elem := range r.Data {
		buf.Write(elem.ToBytes())
	}
	return buf.Bytes()
}

// RESP3Parser parses RESP3 protocol
type RESP3Parser struct {
	reader *bytes.Reader
}

// NewRESP3Parser creates a new RESP3 parser
func NewRESP3Parser(data []byte) *RESP3Parser {
	return &RESP3Parser{reader: bytes.NewReader(data)}
}

// Parse parses RESP3 data and returns the reply
func (p *RESP3Parser) Parse() (redis.Reply, error) {
	// Read type indicator
	ch, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch ch {
	case Resp3SimpleString:
		return p.parseSimpleString()
	case Resp3SimpleError:
		return p.parseSimpleError()
	case Resp3Integer:
		return p.parseInteger()
	case Resp3Null:
		return p.parseNull()
	case Resp3Double:
		return p.parseDouble()
	case Resp3Boolean:
		return p.parseBoolean()
	case Resp3BigNumber:
		return p.parseBigNumber()
	case Resp3BulkString:
		return p.parseBulkString()
	case Resp3Verbatim:
		return p.parseVerbatim()
	case Resp3Array:
		return p.parseArray()
	case Resp3Map:
		return p.parseMap()
	case Resp3Set:
		return p.parseSet()
	case Resp3Push:
		return p.parsePush()
	default:
		return nil, fmt.Errorf("unknown RESP3 type: %c", ch)
	}
}

func (p *RESP3Parser) readLine() ([]byte, error) {
	var line []byte
	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\r' {
			n, err := p.reader.ReadByte()
			if err != nil {
				return nil, err
			}
			if n == '\n' {
				return line, nil
			}
			return nil, errors.New("invalid line ending")
		}
		line = append(line, b)
	}
}

func (p *RESP3Parser) parseSimpleString() (*StatusReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	return MakeStatusReply(string(line)), nil
}

func (p *RESP3Parser) parseSimpleError() (*StandardErrReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	return MakeErrReply(string(line)), nil
}

func (p *RESP3Parser) parseInteger() (*IntReply, error) {
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

func (p *RESP3Parser) parseNull() (*NullReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) != 0 {
		return nil, errors.New("invalid null format")
	}
	return &NullReply{}, nil
}

func (p *RESP3Parser) parseDouble() (*DoubleReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	val, err := strconv.ParseFloat(string(line), 64)
	if err != nil {
		return nil, err
	}
	return MakeDoubleReply(val), nil
}

func (p *RESP3Parser) parseBoolean() (*BooleanReply, error) {
	b, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) != 0 {
		return nil, errors.New("invalid boolean format")
	}
	return MakeBooleanReply(b == 't'), nil
}

func (p *RESP3Parser) parseBigNumber() (*BigNumberReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	return MakeBigNumberReply(string(line)), nil
}

func (p *RESP3Parser) parseBulkString() (redis.Reply, error) {
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
	data := make([]byte, length)
	_, err = p.reader.Read(data)
	if err != nil {
		return nil, err
	}
	// Read \r\n
	p.reader.ReadByte()
	p.reader.ReadByte()
	return MakeBulkReply(data), nil
}

func (p *RESP3Parser) parseVerbatim() (*VerbatimReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}
	data := make([]byte, length)
	_, err = p.reader.Read(data)
	if err != nil {
		return nil, err
	}
	// Read \r\n
	p.reader.ReadByte()
	p.reader.ReadByte()

	// Parse format:value
	colonIdx := bytes.Index(data, []byte(":"))
	if colonIdx < 0 {
		return nil, errors.New("invalid verbatim format")
	}
	format := string(data[:colonIdx])
	value := string(data[colonIdx+1:])
	return MakeVerbatimReply(format, value), nil
}

func (p *RESP3Parser) parseArray() (redis.Reply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return &EmptyMultiBulkReply{}, nil
	}

	args := make([][]byte, length)
	for i := 0; i < length; i++ {
		reply, err := p.Parse()
		if err != nil {
			return nil, err
		}
		if bulk, ok := reply.(*BulkReply); ok {
			args[i] = bulk.Arg
		} else {
			args[i] = reply.ToBytes()
		}
	}
	return MakeMultiBulkReply(args), nil
}

func (p *RESP3Parser) parseMap() (*MapReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}

	m := MakeMapReply()
	for i := 0; i < length; i++ {
		// Parse key
		keyReply, err := p.Parse()
		if err != nil {
			return nil, err
		}
		var key string
		if bulk, ok := keyReply.(*BulkReply); ok {
			key = string(bulk.Arg)
		} else {
			key = string(keyReply.ToBytes())
		}

		// Parse value
		value, err := p.Parse()
		if err != nil {
			return nil, err
		}
		m.Put(key, value)
	}
	return m, nil
}

func (p *RESP3Parser) parseSet() (*SetReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}

	data := make([]redis.Reply, length)
	for i := 0; i < length; i++ {
		reply, err := p.Parse()
		if err != nil {
			return nil, err
		}
		data[i] = reply
	}
	return MakeSetReply(data), nil
}

func (p *RESP3Parser) parsePush() (*PushReply, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}

	// First element is kind
	kindReply, err := p.Parse()
	if err != nil {
		return nil, err
	}
	var kind string
	if bulk, ok := kindReply.(*BulkReply); ok {
		kind = string(bulk.Arg)
	} else {
		kind = string(kindReply.ToBytes())
	}

	// Remaining elements are data
	data := make([]redis.Reply, length-1)
	for i := 0; i < length-1; i++ {
		reply, err := p.Parse()
		if err != nil {
			return nil, err
		}
		data[i] = reply
	}
	return MakePushReply(kind, data), nil
}

func formatFloat(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%.0f", f)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// IsRESP3Enabled returns whether RESP3 is enabled for this connection
// This would be stored in the connection object in practice
func IsRESP3Enabled() bool {
	// Simplified - would check connection's protocol version
	return false
}
