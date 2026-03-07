package stream

import (
	"strconv"
	"testing"
	"time"
)

func TestStreamID_Parse(t *testing.T) {
	lastID := StreamID{1234567890, 5}
	
	tests := []struct {
		input   string
		want    StreamID
		wantErr bool
	}{
		{"1234567890-6", StreamID{1234567890, 6}, false},
		{"1234567890-*", StreamID{1234567890, 6}, false}, // 自动序列号
		{"1234567891-*", StreamID{1234567891, 0}, false}, // 新时间戳
		{"0-0", StreamID{0, 0}, false},                   // 特殊ID（消费者组起始）
		{"invalid", StreamID{}, true},                    // 格式错误
	}
	
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseStreamID(tt.input, lastID)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseStreamID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.Compare(tt.want) != 0 {
				t.Errorf("ParseStreamID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamID_Compare(t *testing.T) {
	tests := []struct {
		a    StreamID
		b    StreamID
		want int
	}{
		{StreamID{1, 0}, StreamID{1, 1}, -1},
		{StreamID{1, 1}, StreamID{1, 0}, 1},
		{StreamID{1, 0}, StreamID{2, 0}, -1},
		{StreamID{2, 0}, StreamID{1, 0}, 1},
		{StreamID{1, 0}, StreamID{1, 0}, 0},
	}
	
	for _, tt := range tests {
		t.Run(tt.a.String()+"_vs_"+tt.b.String(), func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Errorf("Compare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStream_Add(t *testing.T) {
	s := NewStream()
	
	// 测试指定ID（必须先添加，否则自动生成会大于它）
	id2, err := s.Add("1234567890-0", map[string]string{"field": "value2"}, nil)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if id2.Compare(StreamID{1234567890, 0}) != 0 {
		t.Errorf("Add() = %v, want {1234567890 0}", id2)
	}
	
	// 测试自动生成的ID（应该大于上一个ID）
	id1, err := s.Add("*", map[string]string{"field": "value1"}, nil)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if id1.Timestamp == 0 {
		t.Error("Add() generated ID timestamp should not be 0")
	}
	// 验证自动生成的ID大于上一个
	if id1.Compare(id2) <= 0 {
		t.Errorf("Generated ID %v should be greater than last ID %v", id1, id2)
	}
	
	// 测试重复ID（应该失败）
	_, err = s.Add("1234567890-0", map[string]string{"field": "value3"}, nil)
	if err == nil {
		t.Error("Add() should fail with duplicate ID")
	}
}

func TestStream_Range(t *testing.T) {
	s := NewStream()
	
	// 添加测试数据
	for i := 0; i < 5; i++ {
		_, err := s.Add(strconv.FormatInt(int64(1000+i), 10)+"-0",
			map[string]string{"index": strconv.Itoa(i)}, nil)
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}
	
	// 测试范围查询
	entries := s.Range(StreamID{1001, 0}, StreamID{1003, 0})
	if len(entries) != 3 {
		t.Errorf("Range() returned %d entries, want 3", len(entries))
	}
}

func TestConsumerGroup(t *testing.T) {
	s := NewStream()
	
	// 创建消费者组
	err := s.CreateGroup("mygroup", "$")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	
	// 重复创建应该失败
	err = s.CreateGroup("mygroup", "$")
	if err != ErrGroupExists {
		t.Errorf("CreateGroup() error = %v, want ErrGroupExists", err)
	}
	
	// 获取组
	group, err := s.GetGroup("mygroup")
	if err != nil {
		t.Fatalf("GetGroup() error = %v", err)
	}
	if group == nil {
		t.Fatal("GetGroup() returned nil")
	}
	
	// 获取消费者
	consumer := group.GetConsumer("consumer1")
	if consumer == nil {
		t.Fatal("GetConsumer() returned nil")
	}
	if consumer.Name != "consumer1" {
		t.Errorf("Consumer name = %v, want consumer1", consumer.Name)
	}
}

func TestStream_Claim(t *testing.T) {
	s := NewStream()
	
	// 创建消费者组 (使用有效的StreamID)
	err := s.CreateGroup("mygroup", "0-0")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	
	// 添加条目
	id, _ := s.Add("1000-0", map[string]string{"data": "test"}, nil)
	
	group, _ := s.GetGroup("mygroup")
	consumer := group.GetConsumer("consumer1")
	
	// 添加pending条目
	group.Pending[id] = &PendingEntry{
		ID:            id,
		Consumer:      "consumer2",
		DeliveryTime:  time.Now().Add(-time.Hour),
		DeliveryCount: 1,
	}
	
	// 认领条目
	claimed, err := s.Claim("mygroup", "consumer1", time.Minute, []StreamID{id}, &ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 1 {
		t.Errorf("Claim() returned %d entries, want 1", len(claimed))
	}
	
	// 验证消费者变更
	if _, ok := consumer.Pending[id]; !ok {
		t.Error("Claim() did not transfer pending entry to new consumer")
	}
}

func TestStream_Len(t *testing.T) {
	s := NewStream()
	
	if s.Len() != 0 {
		t.Errorf("Len() = %d, want 0", s.Len())
	}
	
	s.Add("*", map[string]string{"field": "value"}, nil)
	s.Add("*", map[string]string{"field": "value2"}, nil)
	
	if s.Len() != 2 {
		t.Errorf("Len() = %d, want 2", s.Len())
	}
}

func TestStream_Delete(t *testing.T) {
	s := NewStream()
	
	id1, _ := s.Add("1000-0", map[string]string{"field": "value1"}, nil)
	s.Add("1001-0", map[string]string{"field": "value2"}, nil)
	
	if s.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", s.Len())
	}
	
	deleted := s.Delete([]StreamID{id1})
	if deleted != 1 {
		t.Errorf("Delete() = %d, want 1", deleted)
	}
	
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
}
