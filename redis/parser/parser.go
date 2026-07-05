package parser

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/redis/protocol"
)

// Upper bounds aligned with Redis proto-max-bulk-len default (512MB).
const (
	maxBulkStringLen int64 = 512 * 1024 * 1024
	maxArrayElements int64 = 1024 * 1024
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload, 1)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

func parse0(rawReader io.Reader, ch chan<- *Payload) {
	closed := false
	closeCh := func() {
		if !closed {
			closed = true
			close(ch)
		}
	}
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
		closeCh()
	}()
	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			closeCh()
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			// there are some empty lines within replication traffic, ignore this error
			//protocolError(ch, "empty line")
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeStatusReply(content),
			}
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parseRDBBulkString(reader, ch)
				if err != nil {
					ch <- &Payload{Err: err}
					closeCh()
					return
				}
			}
		case '-':
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		case '$':
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				closeCh()
				return
			}
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				closeCh()
				return
			}
		case '_':
			ch <- &Payload{Data: &protocol.NullReply{}}
		case '#':
			ch <- &Payload{Data: protocol.MakeBooleanReply(string(line[1:]) == "t")}
		case ',':
			value, err := strconv.ParseFloat(string(line[1:]), 64)
			if err != nil {
				protocolError(ch, "illegal double "+string(line[1:]))
				continue
			}
			ch <- &Payload{Data: protocol.MakeDoubleReply(value)}
		case '(':
			ch <- &Payload{Data: protocol.MakeBigNumberReply(string(line[1:]))}
		case '=':
			err = parseVerbatim(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				closeCh()
				return
			}
		case '%':
			err = parseMap(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				closeCh()
				return
			}
		case '~':
			err = parseSet(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				closeCh()
				return
			}
		case '>':
			err = parsePush(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				closeCh()
				return
			}
		case '|':
			err = parseAttribute(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				closeCh()
				return
			}
		default:
			// Inline command text protocol (e.g. telnet / replication traffic).
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	} else if strLen > maxBulkStringLen {
		protocolError(ch, "bulk string too long")
		return nil
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

// there is no CRLF between RDB and following AOF, therefore it needs to be treated differently
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	header, err := reader.ReadBytes('\n')
	if err != nil {
		return errors.New("failed to read bytes")
	}
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	if len(header) == 0 {
		return errors.New("empty header")
	}
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen <= 0 {
		return errors.New("illegal bulk header: " + string(header))
	}
	if strLen > maxBulkStringLen {
		return errors.New("bulk string too long")
	}
	body := make([]byte, strLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)]),
	}
	return nil
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	} else if nStrs > maxArrayElements {
		protocolError(ch, "array too long")
		return nil
	}
	replies := make([]redis.Reply, 0, nStrs)
	allBulk := true
	for i := int64(0); i < nStrs; i++ {
		reply, err := parseElement(reader)
		if err != nil {
			return err
		}
		replies = append(replies, reply)
		switch reply.(type) {
		case *protocol.BulkReply, *protocol.NullBulkReply:
		default:
			allBulk = false
		}
	}
	if allBulk {
		lines := make([][]byte, nStrs)
		for i, reply := range replies {
			if br, ok := reply.(*protocol.BulkReply); ok {
				lines[i] = br.Arg
			}
			// NullBulkReply leaves lines[i] as nil, matching MakeMultiBulkReply semantics
		}
		ch <- &Payload{
			Data: protocol.MakeMultiBulkReply(lines),
		}
	} else {
		ch <- &Payload{
			Data: protocol.MakeMultiRawReply(replies),
		}
	}
	return nil
}

func parseElement(reader *bufio.Reader) (redis.Reply, error) {
	ch, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	return parseRESP3Value(reader, ch)
}

func parseRESP3Value(reader *bufio.Reader, ch byte) (redis.Reply, error) {
	switch ch {
	case '+':
		line, err := readCRLFLine(reader)
		if err != nil {
			return nil, err
		}
		return protocol.MakeStatusReply(string(line)), nil
	case '-':
		line, err := readCRLFLine(reader)
		if err != nil {
			return nil, err
		}
		return protocol.MakeErrReply(string(line)), nil
	case ':':
		line, err := readCRLFLine(reader)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("illegal integer: %w", err)
		}
		return protocol.MakeIntReply(value), nil
	case '_':
		_, err := readCRLFLine(reader)
		return &protocol.NullReply{}, err
	case '#':
		line, err := readCRLFLine(reader)
		if err != nil {
			return nil, err
		}
		return protocol.MakeBooleanReply(string(line) == "t"), nil
	case ',':
		line, err := readCRLFLine(reader)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseFloat(string(line), 64)
		if err != nil {
			return nil, fmt.Errorf("illegal double: %w", err)
		}
		return protocol.MakeDoubleReply(value), nil
	case '(':
		line, err := readCRLFLine(reader)
		if err != nil {
			return nil, err
		}
		return protocol.MakeBigNumberReply(string(line)), nil
	case '$':
		return parseBulkStringFull(reader)
	case '=':
		return parseVerbatimFull(reader)
	case '*':
		return parseAggregate(reader, '*')
	case '~':
		return parseAggregate(reader, '~')
	case '>':
		return parseAggregate(reader, '>')
	case '%':
		return parseMapFull(reader)
	case '|':
		return parseAttributeFull(reader)
	default:
		return nil, fmt.Errorf("unknown RESP type: %c", ch)
	}
}

func readCRLFLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("invalid line ending")
	}
	return bytes.TrimSuffix(line, []byte{'\r', '\n'}), nil
}

func parseBulkStringFull(reader *bufio.Reader) (redis.Reply, error) {
	header, err := readCRLFLine(reader)
	if err != nil {
		return nil, err
	}
	strLen, err := strconv.ParseInt(string(header), 10, 64)
	if err != nil || strLen < -1 {
		return nil, errors.New("illegal bulk string header")
	}
	if strLen == -1 {
		return protocol.MakeNullBulkReply(), nil
	}
	if strLen > maxBulkStringLen {
		return nil, errors.New("bulk string too long")
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, err
	}
	return protocol.MakeBulkReply(body[:len(body)-2]), nil
}

func parseVerbatim(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	reply, err := parseVerbatimFromHeader(header, reader)
	if err != nil {
		return err
	}
	ch <- &Payload{Data: reply}
	return nil
}

func parseVerbatimFull(reader *bufio.Reader) (redis.Reply, error) {
	header, err := readCRLFLine(reader)
	if err != nil {
		return nil, err
	}
	return parseVerbatimFromHeader(header, reader)
}

func parseVerbatimFromHeader(header []byte, reader *bufio.Reader) (redis.Reply, error) {
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < 0 {
		return nil, errors.New("illegal verbatim string header")
	}
	if strLen > maxBulkStringLen {
		return nil, errors.New("verbatim string too long")
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, err
	}
	content := string(body[:len(body)-2])
	idx := strings.IndexByte(content, ':')
	if idx < 0 {
		return nil, errors.New("invalid verbatim string format")
	}
	return protocol.MakeVerbatimReply(content[:idx], content[idx+1:]), nil
}

func parseMap(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	reply, err := parseMapFromHeader(header, reader)
	if err != nil {
		return err
	}
	ch <- &Payload{Data: reply}
	return nil
}

func parseMapFull(reader *bufio.Reader) (redis.Reply, error) {
	header, err := readCRLFLine(reader)
	if err != nil {
		return nil, err
	}
	return parseMapFromHeader(header, reader)
}

func parseMapFromHeader(header []byte, reader *bufio.Reader) (redis.Reply, error) {
	n, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || n < 0 {
		return nil, errors.New("illegal map header")
	}
	if n > maxArrayElements {
		return nil, errors.New("map too long")
	}
	m := protocol.MakeMapReply()
	for i := int64(0); i < n; i++ {
		keyReply, err := parseElement(reader)
		if err != nil {
			return nil, err
		}
		valueReply, err := parseElement(reader)
		if err != nil {
			return nil, err
		}
		var key string
		switch k := keyReply.(type) {
		case *protocol.BulkReply:
			key = string(k.Arg)
		case *protocol.StatusReply:
			key = k.Status
		default:
			key = string(keyReply.ToBytes())
		}
		m.Put(key, valueReply)
	}
	return m, nil
}

func parseSet(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	reply, err := parseAggregateFromHeader(header, reader, '~')
	if err != nil {
		return err
	}
	if set, ok := reply.(*protocol.SetReply); ok {
		ch <- &Payload{Data: set}
		return nil
	}
	return errors.New("internal error: expected SetReply")
}

func parsePush(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	reply, err := parseAggregateFromHeader(header, reader, '>')
	if err != nil {
		return err
	}
	push, ok := reply.(*protocol.PushReply)
	if !ok {
		return errors.New("internal error: expected PushReply from push")
	}
	kind := push.Kind
	var data []redis.Reply
	if kind == "" && len(push.Data) > 0 {
		// parseAggregateFromHeader returns a generic PushReply with an empty Kind;
		// the first element is the push type.
		if br, ok := push.Data[0].(*protocol.BulkReply); ok {
			kind = string(br.Arg)
		}
		if len(push.Data) > 1 {
			data = push.Data[1:]
		}
	} else {
		data = push.Data
	}
	ch <- &Payload{Data: protocol.MakePushReply(kind, data)}
	return nil
}

func parseAttribute(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	reply, err := parseAttributeFromHeader(header, reader)
	if err != nil {
		return err
	}
	ch <- &Payload{Data: reply}
	return nil
}

func parseAttributeFull(reader *bufio.Reader) (redis.Reply, error) {
	header, err := readCRLFLine(reader)
	if err != nil {
		return nil, err
	}
	return parseAttributeFromHeader(header, reader)
}

func parseAttributeFromHeader(header []byte, reader *bufio.Reader) (redis.Reply, error) {
	n, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || n < 0 {
		return nil, errors.New("illegal attribute header")
	}
	attrs := protocol.MakeMapReply()
	for i := int64(0); i < n; i++ {
		keyReply, err := parseElement(reader)
		if err != nil {
			return nil, err
		}
		valueReply, err := parseElement(reader)
		if err != nil {
			return nil, err
		}
		var key string
		switch k := keyReply.(type) {
		case *protocol.BulkReply:
			key = string(k.Arg)
		case *protocol.StatusReply:
			key = k.Status
		default:
			key = string(keyReply.ToBytes())
		}
		attrs.Put(key, valueReply)
	}
	content, err := parseElement(reader)
	if err != nil {
		return nil, err
	}
	return protocol.MakeAttributeReply(attrs, content), nil
}

func parseAggregate(reader *bufio.Reader, typ byte) (redis.Reply, error) {
	header, err := readCRLFLine(reader)
	if err != nil {
		return nil, err
	}
	// parseAggregate is called after the type byte has already been consumed,
	// so reconstruct a header that includes the type prefix.
	fullHeader := append([]byte{typ}, header...)
	return parseAggregateFromHeader(fullHeader, reader, typ)
}

func parseAggregateFromHeader(header []byte, reader *bufio.Reader, typ byte) (redis.Reply, error) {
	n, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || n < 0 {
		return nil, errors.New("illegal aggregate header")
	}
	if n > maxArrayElements {
		return nil, errors.New("aggregate too long")
	}
	if n == 0 {
		switch typ {
		case '~':
			return protocol.MakeSetReply(nil), nil
		case '>':
			return protocol.MakePushReply("", nil), nil
		default:
			return protocol.MakeEmptyMultiBulkReply(), nil
		}
	}
	replies := make([]redis.Reply, 0, n)
	for i := int64(0); i < n; i++ {
		reply, err := parseElement(reader)
		if err != nil {
			return nil, err
		}
		replies = append(replies, reply)
	}
	switch typ {
	case '~':
		return protocol.MakeSetReply(replies), nil
	case '>':
		return protocol.MakePushReply("", replies), nil
	default:
		return protocol.MakeMultiRawReply(replies), nil
	}
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
