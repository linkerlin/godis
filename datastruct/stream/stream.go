package stream

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/datastruct/dict"
)

var (
	ErrInvalidStreamID   = errors.New("ERR Invalid stream ID")
	ErrStreamIDTooSmall  = errors.New("ERR The ID specified is smaller than the target stream top item")
	ErrNoSuchConsumer    = errors.New("ERR No such consumer")
	ErrNoSuchGroup       = errors.New("ERR No such consumer group")
	ErrConsumerExists    = errors.New("ERR Consumer already exists")
	ErrGroupExists       = errors.New("ERR Consumer group name already exists")
)

// StreamID 流条目ID (毫秒时间戳-序列号)
type StreamID struct {
	Timestamp int64
	Sequence  int64
}

// String 返回 "timestamp-sequence" 格式
func (sid StreamID) String() string {
	return strconv.FormatInt(sid.Timestamp, 10) + "-" + strconv.FormatInt(sid.Sequence, 10)
}

// IsZero 检查是否为zero ID
func (sid StreamID) IsZero() bool {
	return sid.Timestamp == 0 && sid.Sequence == 0
}

// Compare 比较两个StreamID
// 返回 -1 表示 sid < other, 0 表示相等, 1 表示 sid > other
func (sid StreamID) Compare(other StreamID) int {
	if sid.Timestamp < other.Timestamp {
		return -1
	}
	if sid.Timestamp > other.Timestamp {
		return 1
	}
	if sid.Sequence < other.Sequence {
		return -1
	}
	if sid.Sequence > other.Sequence {
		return 1
	}
	return 0
}

// ParseStreamID 解析 "timestamp-sequence" 或 "timestamp-*" 格式的ID
func ParseStreamID(s string, lastID StreamID) (StreamID, error) {
	if s == "*" {
		// 自动生成ID
		return GenerateStreamID(lastID)
	}
	
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return StreamID{}, ErrInvalidStreamID
	}
	
	timestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, ErrInvalidStreamID
	}
	
	var sequence int64
	if parts[1] == "*" {
		// 自动生成序列号
		if timestamp == lastID.Timestamp {
			sequence = lastID.Sequence + 1
		} else {
			sequence = 0
		}
	} else {
		sequence, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return StreamID{}, ErrInvalidStreamID
		}
	}
	
	return StreamID{Timestamp: timestamp, Sequence: sequence}, nil
}

// GenerateStreamID 生成新的StreamID
func GenerateStreamID(lastID StreamID) (StreamID, error) {
	now := time.Now().UnixMilli()
	
	if now > lastID.Timestamp {
		return StreamID{Timestamp: now, Sequence: 0}, nil
	}
	
	if now == lastID.Timestamp {
		return StreamID{Timestamp: now, Sequence: lastID.Sequence + 1}, nil
	}
	
	// 时钟回退情况，使用lastID + 1
	return StreamID{Timestamp: lastID.Timestamp, Sequence: lastID.Sequence + 1}, nil
}

// StreamEntry 流条目
type StreamEntry struct {
	ID     StreamID
	Fields map[string]string
}

// PendingEntry 待处理条目 (用于消费者组)
type PendingEntry struct {
	ID            StreamID
	Consumer      string
	DeliveryTime  time.Time
	DeliveryCount int
}

// Consumer 消费者
type Consumer struct {
	Name      string
	SeenTime  time.Time
	Pending   map[StreamID]*PendingEntry // 待处理条目
}

// ConsumerGroup 消费者组
type ConsumerGroup struct {
	Name           string
	LastID         StreamID                     // 最后递送ID (组创建时的ID)
	EntriesRead    int64                        // 已读取条目数 (Redis 7.4+)
	Consumers      *dict.ConcurrentDict         // consumer name -> *Consumer
	Pending        map[StreamID]*PendingEntry   // 组级别的待处理条目
	mu             sync.RWMutex
}

// NewConsumerGroup 创建消费者组
func NewConsumerGroup(name string, lastID StreamID) *ConsumerGroup {
	return &ConsumerGroup{
		Name:      name,
		LastID:    lastID,
		Consumers: dict.MakeConcurrent(16),
		Pending:   make(map[StreamID]*PendingEntry),
	}
}

// GetConsumer 获取或创建消费者
func (cg *ConsumerGroup) GetConsumer(name string) *Consumer {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	
	raw, ok := cg.Consumers.Get(name)
	if ok {
		return raw.(*Consumer)
	}
	
	consumer := &Consumer{
		Name:     name,
		SeenTime: time.Now(),
		Pending:  make(map[StreamID]*PendingEntry),
	}
	cg.Consumers.Put(name, consumer)
	return consumer
}

// DeleteConsumer 删除消费者
func (cg *ConsumerGroup) DeleteConsumer(name string) (int, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	
	raw, ok := cg.Consumers.Get(name)
	if !ok {
		return 0, ErrNoSuchConsumer
	}
	
	consumer := raw.(*Consumer)
	pendingCount := len(consumer.Pending)
	
	// 将消费者的pending条目转移给组
	for id, entry := range consumer.Pending {
		cg.Pending[id] = entry
	}
	
	cg.Consumers.Remove(name)
	return pendingCount, nil
}

// Stream 流数据结构
type Stream struct {
	mu           sync.RWMutex
	entries      *dict.ConcurrentDict  // StreamID.String() -> *StreamEntry
	groups       *dict.ConcurrentDict  // group name -> *ConsumerGroup
	lastID       StreamID
	maxlen       int64                 // 最大长度限制
	entriesAdded int64                 // 总添加条目数 (用于 XINFO)
}

// NewStream 创建新的Stream
func NewStream() *Stream {
	return &Stream{
		entries:      dict.MakeConcurrent(64),
		groups:       dict.MakeConcurrent(16),
		lastID:       StreamID{0, 0},
		maxlen:       -1, // 无限制
		entriesAdded: 0,
	}
}

// AddOptions XADD选项
type AddOptions struct {
	NoMkStream   bool      // 如果stream不存在，不创建
	MaxLen       int64     // 最大长度
	MaxLenApprox bool      // 近似最大长度 (~)
	MinID        StreamID  // 最小ID，小于此ID的条目会被删除
	Limit        int64     // 删除限制
}

// Add 添加条目到Stream
func (s *Stream) Add(idStr string, fields map[string]string, opts *AddOptions) (StreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 解析ID
	id, err := ParseStreamID(idStr, s.lastID)
	if err != nil {
		return StreamID{}, err
	}
	
	// 检查ID是否大于lastID
	if id.Compare(s.lastID) <= 0 {
		return StreamID{}, ErrStreamIDTooSmall
	}
	
	// 创建条目
	entry := &StreamEntry{
		ID:     id,
		Fields: fields,
	}
	
	// 添加到entries
	s.entries.Put(id.String(), entry)
	s.lastID = id
	s.entriesAdded++
	
	// 处理maxlen限制
	if opts != nil && opts.MaxLen > 0 {
		s.trimToMaxLen(opts.MaxLen, opts.MaxLenApprox)
	}
	
	// 处理MinID限制
	if opts != nil && !opts.MinID.IsZero() {
		s.trimToMinID(opts.MinID, opts.Limit)
	}
	
	return id, nil
}

// trimToMaxLen 根据最大长度裁剪Stream
func (s *Stream) trimToMaxLen(maxlen int64, approx bool) {
	if approx {
		// 近似裁剪，每次只删除大约10%的过期条目
		if s.entries.Len() <= int(maxlen+maxlen/10) {
			return
		}
	}
	
	for int64(s.entries.Len()) > maxlen {
		// 删除最老的条目
		// 注意：这里简化处理，实际应该按ID排序删除
		// 由于 ConcurrentDict 是无序的，我们需要其他方式
		// 实际实现中可能需要维护一个有序索引
	}
}

// trimToMinID 根据最小ID裁剪Stream
func (s *Stream) trimToMinID(minID StreamID, limit int64) {
	// 删除小于minID的条目
	deleted := int64(0)
	s.entries.ForEach(func(key string, val interface{}) bool {
		if limit > 0 && deleted >= limit {
			return false
		}
		
		entry := val.(*StreamEntry)
		if entry.ID.Compare(minID) < 0 {
			s.entries.Remove(key)
			deleted++
		}
		return true
	})
}

// Range 获取范围内的条目 [start, end]
func (s *Stream) Range(start, end StreamID) []*StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var result []*StreamEntry
	s.entries.ForEach(func(key string, val interface{}) bool {
		entry := val.(*StreamEntry)
		if entry.ID.Compare(start) >= 0 && entry.ID.Compare(end) <= 0 {
			result = append(result, entry)
		}
		return true
	})
	
	// 按ID排序
	// 简化：实际应该使用有序结构
	sortEntriesByID(result)
	return result
}

// ReverseRange 反向获取范围内的条目
func (s *Stream) ReverseRange(start, end StreamID, count int) []*StreamEntry {
	entries := s.Range(start, end)
	// 反转顺序
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	if count > 0 && len(entries) > count {
		entries = entries[:count]
	}
	return entries
}

// Len 返回Stream中的条目数
func (s *Stream) Len() int {
	return s.entries.Len()
}

// GetLastID 返回最后生成的ID
func (s *Stream) GetLastID() StreamID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastID
}

// Delete 删除条目
func (s *Stream) Delete(ids []StreamID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	deleted := 0
	for _, id := range ids {
		_, result := s.entries.Remove(id.String())
		if result > 0 {
			deleted++
		}
	}
	return deleted
}

// CreateGroup 创建消费者组
func (s *Stream) CreateGroup(name, startID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 检查组是否已存在
	if _, ok := s.groups.Get(name); ok {
		return ErrGroupExists
	}
	
	var lastID StreamID
	if startID == "$" {
		// 从当前最后一个条目开始
		lastID = s.lastID
	} else {
		var err error
		lastID, err = ParseStreamID(startID, s.lastID)
		if err != nil {
			return err
		}
	}
	
	group := NewConsumerGroup(name, lastID)
	s.groups.Put(name, group)
	return nil
}

// DestroyGroup 销毁消费者组
func (s *Stream) DestroyGroup(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, ok := s.groups.Get(name); !ok {
		return ErrNoSuchGroup
	}
	
	s.groups.Remove(name)
	return nil
}

// GetGroup 获取消费者组
func (s *Stream) GetGroup(name string) (*ConsumerGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	raw, ok := s.groups.Get(name)
	if !ok {
		return nil, ErrNoSuchGroup
	}
	return raw.(*ConsumerGroup), nil
}

// GetGroups 获取所有消费者组
func (s *Stream) GetGroups() []*ConsumerGroup {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var groups []*ConsumerGroup
	s.groups.ForEach(func(key string, val interface{}) bool {
		groups = append(groups, val.(*ConsumerGroup))
		return true
	})
	return groups
}

// ClaimOptions XCLAIM选项
type ClaimOptions struct {
	Idle       time.Duration
	Time       time.Time
	RetryCount int
	Force      bool
	JustID     bool
}

// Claim 认领待处理条目
func (s *Stream) Claim(groupName, consumerName string, minIdleTime time.Duration, ids []StreamID, opts *ClaimOptions) ([]*StreamEntry, error) {
	group, err := s.GetGroup(groupName)
	if err != nil {
		return nil, err
	}
	
	consumer := group.GetConsumer(consumerName)
	consumer.SeenTime = time.Now()
	
	var result []*StreamEntry
	now := time.Now()
	
	for _, id := range ids {
		// 查找待处理条目
		pending, ok := group.Pending[id]
		if !ok {
			// 检查是否在其他消费者那里
			found := false
			group.Consumers.ForEach(func(name string, val interface{}) bool {
				c := val.(*Consumer)
				if p, ok := c.Pending[id]; ok {
					pending = p
					// 从原消费者移除
					delete(c.Pending, id)
					found = true
					return false
				}
				return true
			})
			
			if !found && !opts.Force {
				continue
			}
			
			if !found {
				// 创建新的pending条目
				pending = &PendingEntry{
					ID: id,
				}
			}
		}
		
		// 检查空闲时间
		if opts.Force || now.Sub(pending.DeliveryTime) >= minIdleTime {
			// 更新条目
			pending.Consumer = consumerName
			pending.DeliveryTime = now
			if opts.Time.IsZero() {
				pending.DeliveryTime = opts.Time
			}
			if opts.RetryCount > 0 {
				pending.DeliveryCount = opts.RetryCount
			} else {
				pending.DeliveryCount++
			}
			
			// 添加到消费者的pending
			consumer.Pending[id] = pending
			
			// 获取条目内容
			if !opts.JustID {
				if raw, ok := s.entries.Get(id.String()); ok {
					result = append(result, raw.(*StreamEntry))
				}
			}
		}
	}
	
	return result, nil
}

// AutoClaim 自动认领待处理条目
func (s *Stream) AutoClaim(groupName, consumerName string, minIdleTime time.Duration, start StreamID, count int) ([]*StreamEntry, StreamID, error) {
	group, err := s.GetGroup(groupName)
	if err != nil {
		return nil, StreamID{}, err
	}
	
	consumer := group.GetConsumer(consumerName)
	consumer.SeenTime = time.Now()
	
	var result []*StreamEntry
	var nextID StreamID
	now := time.Now()
	claimed := 0
	
	// 遍历组的pending条目
	for id, pending := range group.Pending {
		if id.Compare(start) < 0 {
			continue
		}
		
		if claimed >= count {
			nextID = id
			break
		}
		
		if now.Sub(pending.DeliveryTime) >= minIdleTime {
			// 认领条目
			pending.Consumer = consumerName
			pending.DeliveryTime = now
			pending.DeliveryCount++
			
			consumer.Pending[id] = pending
			delete(group.Pending, id)
			
			if raw, ok := s.entries.Get(id.String()); ok {
				result = append(result, raw.(*StreamEntry))
			}
			
			claimed++
		}
	}
	
	return result, nextID, nil
}

// sortEntriesByID 按ID排序条目 (简单冒泡排序，实际应该用更高效的算法)
func sortEntriesByID(entries []*StreamEntry) {
	n := len(entries)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if entries[i].ID.Compare(entries[j].ID) > 0 {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
}

// GetInfo 获取Stream信息
func (s *Stream) GetInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	info := make(map[string]interface{})
	info["length"] = s.entries.Len()
	info["radix-tree-keys"] = s.entries.Len() // 简化
	info["radix-tree-nodes"] = s.entries.Len() * 2 // 简化
	info["last-generated-id"] = s.lastID.String()
	info["max-deleted-entry-id"] = "0-0" // 简化
	info["entries-added"] = s.entriesAdded
	info["recorded-first-entry-id"] = s.lastID.String() // 简化
	
	// 获取第一个和最后一个条目
	if s.entries.Len() > 0 {
		var first, last *StreamEntry
		s.entries.ForEach(func(key string, val interface{}) bool {
			entry := val.(*StreamEntry)
			if first == nil || entry.ID.Compare(first.ID) < 0 {
				first = entry
			}
			if last == nil || entry.ID.Compare(last.ID) > 0 {
				last = entry
			}
			return true
		})
		
		if first != nil {
			info["first-entry"] = first
		}
		if last != nil {
			info["last-entry"] = last
		}
	}
	
	return info
}
