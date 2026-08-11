package streaming

import (
	"sync"
	"testing"
	"time"
)

func TestCachePutGet(t *testing.T) {
	cache := NewReadAheadCache(10*1024*1024, 1024*1024)

	cache.Put("test.mkv", 0, []byte("hello world"))

	buf := make([]byte, 11)
	n := cache.CopyTo("test.mkv", buf, 0)
	if n != 11 {
		t.Errorf("expected 11 bytes, got %d", n)
	}
	if string(buf) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(buf))
	}
}

func TestCacheMiss(t *testing.T) {
	cache := NewReadAheadCache(10*1024*1024, 1024*1024)

	buf := make([]byte, 11)
	n := cache.CopyTo("test.mkv", buf, 0)
	if n != 0 {
		t.Errorf("expected 0 bytes on miss, got %d", n)
	}
}

func TestCacheCovered(t *testing.T) {
	cache := NewReadAheadCache(10*1024*1024, 1024*1024)

	if cache.Covered("test.mkv", 0, 100) {
		t.Error("expected not covered before Put")
	}

	cache.Put("test.mkv", 0, make([]byte, 100))

	if !cache.Covered("test.mkv", 0, 100) {
		t.Error("expected covered after Put")
	}
}

func TestCacheMaxOffset(t *testing.T) {
	cache := NewReadAheadCache(10*1024*1024, 1024*1024)

	cache.Put("test.mkv", 0, make([]byte, 100))
	cache.Put("test.mkv", 200, make([]byte, 50))

	max := cache.MaxCachedOffset("test.mkv")
	if max != 250 {
		t.Errorf("expected max offset 250, got %d", max)
	}
}

func TestCacheEviction(t *testing.T) {
	// Small budget to trigger eviction
	cache := NewReadAheadCache(2048, 1024)

	// Fill beyond budget
	for i := 0; i < 5; i++ {
		offset := int64(i * 1024)
		cache.Put("test.mkv", offset, make([]byte, 1024))
	}

	// Cache should have evicted some entries
	used, entries := cache.Stats()
	if used > 2048*2 {
		t.Errorf("expected eviction, but used=%d", used)
	}
	t.Logf("Cache stats: used=%d, entries=%d", used, entries)
}

func TestCacheConcurrent(t *testing.T) {
	cache := NewReadAheadCache(10*1024*1024, 1024*1024)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			offset := int64(idx * 1024)
			data := make([]byte, 1024)
			data[0] = byte(idx)
			cache.Put("test.mkv", offset, data)

			buf := make([]byte, 1024)
			cache.CopyTo("test.mkv", buf, offset)
		}(i)
	}
	wg.Wait()
}

func TestChunkSize(t *testing.T) {
	cache := NewReadAheadCache(10*1024*1024, 1024*1024)

	// Default chunk size
	if cache.ChunkSize("test.mkv") != 1024*1024 {
		t.Error("wrong default chunk size")
	}

	// Set adaptive chunk size
	cache.SetPieceLen("test.mkv", 4*1024*1024)
	if cache.ChunkSize("test.mkv") != 4*1024*1024 {
		t.Error("wrong adaptive chunk size")
	}
}

func TestMasterSemaphore(t *testing.T) {
	sem := NewMasterSemaphore(3)

	// Should acquire 3
	if !sem.Acquire() { t.Error("failed to acquire 1") }
	if !sem.Acquire() { t.Error("failed to acquire 2") }
	if !sem.Acquire() { t.Error("failed to acquire 3") }

	// Should fail (full)
	if sem.Acquire() {
		t.Error("should have failed to acquire 4th")
	}

	// Release one
	sem.Release()

	// Should succeed now
	if !sem.Acquire() {
		t.Error("failed to acquire after release")
	}
}

func TestFetchFlightDedup(t *testing.T) {
	dedup := &FetchFlightDedup{}

	f1, isLeader := dedup.Start("test.mkv", 0)
	if !isLeader {
		t.Error("first caller should be leader")
	}

	f2, isLeader := dedup.Start("test.mkv", 0)
	if isLeader {
		t.Error("second caller should not be leader")
	}
	if f1 == f2 {
		// Different flight objects expected
	}

	dedup.Complete("test.mkv", 0, 1024, nil)
}

func TestBufferPool(t *testing.T) {
	pool := NewReadBufferPool(1024)

	buf := pool.Get()
	if len(*buf) != 1024 {
		t.Errorf("expected buffer size 1024, got %d", len(*buf))
	}
	pool.Put(buf)
}

func TestItoa(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{123, "123"},
		{9999999999, "9999999999"},
	}
	for _, c := range cases {
		got := itoa(c.n)
		if got != c.want {
			t.Errorf("itoa(%d) = %s, want %s", c.n, got, c.want)
		}
	}
}

func TestSwitchContext(t *testing.T) {
	cache := NewReadAheadCache(10*1024*1024, 1024*1024)

	cache.Put("file1.mkv", 0, make([]byte, 100))
	cache.Put("file2.mkv", 0, make([]byte, 100))

	// Switch to file1
	cache.SwitchContext("file1.mkv")
	time.Sleep(10 * time.Millisecond) // Let session propagate

	// file1 data should still be there (same session)
	if !cache.Covered("file1.mkv", 0, 100) {
		t.Error("file1 data lost after switch")
	}
}
