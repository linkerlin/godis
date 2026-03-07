package client

import (
	"net"
	"testing"
)

// skipIfNoRedis skips the test if no Redis server is available
func skipIfNoRedis(t *testing.T) {
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Skip("skipping test: Redis server not available at localhost:6379")
	}
	conn.Close()
}
