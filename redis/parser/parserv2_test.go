package parser

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkParseSETCommand(b *testing.B) {
	valueSizes := []int{10, 100, 1000, 10000}

	for _, size := range valueSizes {
		value := bytes.Repeat([]byte("a"), size)
		cmd := []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$%d\r\n%s\r\n", len(value), value))

		b.Run("value_size_"+formatSize(size), func(subB *testing.B) {
			subB.ResetTimer()

			for i := 0; i < subB.N; i++ {
				reader := bytes.NewReader(cmd)
				_, err := ParseV2(reader)
				if err != nil {
					subB.Fatalf("parse failed: %v", err)
				}
			}
		})
	}
}

func TestParseV2(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 100)
	data := []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$%d\r\n%s\r\n", len(value), value))
	cmdLine, err := ParseV2(bytes.NewBuffer(data))
	if err != nil {
		t.Error(err)
		return
	}
	if len(cmdLine) != 3 || string(cmdLine[0]) != "SET" || string(cmdLine[1]) != "key" || string(cmdLine[2]) != string(value) {
		t.Error("parse error")
		return
	}
}

func TestParseFrame_HalfPacketAndPipeline(t *testing.T) {
	value := bytes.Repeat([]byte("x"), 256)
	frame1 := []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$%d\r\n%s\r\n", len(value), value))
	frame2 := []byte("*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n")

	// Every prefix strictly shorter than len(frame1) must report complete=false, err=nil
	for cut := 1; cut < len(frame1); cut++ {
		cmd, consumed, complete, err := ParseFrame(frame1[:cut])
		if err != nil {
			t.Fatalf("cut=%d unexpected err: %v", cut, err)
		}
		if complete || consumed != 0 || cmd != nil {
			t.Fatalf("cut=%d expected incomplete frame, got complete=%v consumed=%d", cut, complete, consumed)
		}
	}

	// Pipeline of frame1 + frame2 + half of frame1
	stream := append(append([]byte{}, frame1...), frame2...)
	stream = append(stream, frame1[:17]...)

	cmd1, n1, ok1, err1 := ParseFrame(stream)
	if err1 != nil || !ok1 || n1 != len(frame1) || len(cmd1) != 3 || string(cmd1[1]) != "key1" {
		t.Fatalf("frame1 parse failed: ok=%v n=%d err=%v", ok1, n1, err1)
	}
	stream = stream[n1:]

	cmd2, n2, ok2, err2 := ParseFrame(stream)
	if err2 != nil || !ok2 || n2 != len(frame2) || len(cmd2) != 2 || string(cmd2[0]) != "GET" {
		t.Fatalf("frame2 parse failed: ok=%v n=%d err=%v", ok2, n2, err2)
	}
	stream = stream[n2:]

	// Remaining 17 bytes are an incomplete frame
	_, n3, ok3, err3 := ParseFrame(stream)
	if err3 != nil || ok3 || n3 != 0 {
		t.Fatalf("expected trailing half-frame to wait, got ok=%v n=%d err=%v", ok3, n3, err3)
	}
}

func formatSize(size int) string {
	units := []string{"B", "KB", "MB"}
	unitIndex := 0
	floatSize := float64(size)

	for floatSize >= 1024 && unitIndex < len(units)-1 {
		floatSize /= 1024
		unitIndex++
	}

	return fmt.Sprintf("%.0f%s", floatSize, units[unitIndex])
}
