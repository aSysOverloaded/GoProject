package main

import (
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// freeAddr reserves a local port and releases it, so a server can be started
// on it later.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// TestConnectRedisWaitsForRedisThatStartsLate covers a consumer that gives up
// on its first connection attempt. On Kubernetes the consumer pods started
// before Redis was ready, exited, and crash-looped until it was; the smoke
// test failed the same way whenever Redis took more than a moment to answer.
// A Redis that comes up a couple of seconds late must not kill the process.
func TestConnectRedisWaitsForRedisThatStartsLate(t *testing.T) {
	addr := freeAddr(t)
	server := miniredis.NewMiniRedis()
	t.Cleanup(server.Close)

	started := make(chan error, 1)
	go func() {
		// Later than a single connection attempt can cover: one Ping with a
		// 3s timeout, including go-redis's own internal retries. Only a real
		// retry loop survives this. (With a 2s delay a single attempt
		// sometimes squeaked through on Windows, where a refused connection
		// is slow to report - which made the test prove nothing.)
		time.Sleep(4 * time.Second)
		started <- server.StartAddr(addr)
	}()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	if err := connectRedis(rdb, 15*time.Second); err != nil {
		t.Fatalf("gave up on a Redis that came up 4s late: %v", err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("test server failed to start: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("test server never started")
	}
}

// TestConnectRedisGivesUpAtItsDeadline guards the other side: with nothing
// ever listening, startup must fail within its budget rather than hang.
func TestConnectRedisGivesUpAtItsDeadline(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: freeAddr(t)})
	t.Cleanup(func() { _ = rdb.Close() })

	start := time.Now()
	err := connectRedis(rdb, time.Second)
	if err == nil {
		t.Fatal("connected to a port nothing listens on")
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Errorf("took %v to give up with a 1s budget", elapsed)
	}
}
