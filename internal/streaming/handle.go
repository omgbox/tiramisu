package streaming

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	fetchMaxWaitS   = 10 // max seconds for a single FetchBlock fallback (was 30, too slow for open)
	fetchRetryDelay = 200 * time.Millisecond // retry faster (was 1s)
)

// MkvHandle represents an open file handle for a virtual file.
// Fields ordered largest→smallest for cache-line efficiency.
type MkvHandle struct {
	client  NativeClient    // 8 bytes (pointer)
	raCache *ReadAheadCache // 8 bytes (pointer)
	pump    *AdaptivePump   // 8 bytes (pointer)
	mu      sync.Mutex      // 8 bytes (state + sema)
	url     string          // 16 bytes (pointer + len)
	magnet  string          // 16 bytes (pointer + len)
	path    string          // 16 bytes (pointer + len)
	hash    string          // 16 bytes (pointer + len)
	size    int64           // 8 bytes
	lastOff int64           // 8 bytes

	// Grouped atomics
	lastActivityTime int64 // atomic, unixnano
	fileID           int
	closed           atomic.Bool
}

// HandleConfig holds parameters for creating a new handle.
type HandleConfig struct {
	Path     string
	URL      string
	Magnet   string
	Size     int64
	Hash     string
	FilePath string
	Client   NativeClient
	Cache    *ReadAheadCache
	PumpSem  *MasterSemaphore
}

// NewHandle creates a MkvHandle with an AdaptivePump.
func NewHandle(cfg HandleConfig) *MkvHandle {
	h := &MkvHandle{
		path:             cfg.Path,
		url:              cfg.URL,
		magnet:           cfg.Magnet,
		size:             cfg.Size,
		hash:             cfg.Hash,
		client:           cfg.Client,
		raCache:          cfg.Cache,
		lastActivityTime: time.Now().UnixNano(),
		lastOff:          -1,
	}

	if cfg.Client == nil || cfg.Magnet == "" {
		return h
	}

	// Wake the torrent engine — connect to peers and load metadata.
	// addTorrent() pre-wakes on startup, so this is usually instant.
	if err := cfg.Client.Wake(cfg.Magnet, 0); err != nil {
		log.Printf("[Handle] Wake failed for %s: %v", cfg.Path, err)
		return h
	}

	if cfg.FilePath != "" {
		if fid, err := cfg.Client.FindFileID(cfg.Hash, cfg.FilePath); err == nil {
			h.fileID = fid
		} else {
			log.Printf("[Handle] FindFileID failed: %v (fallback 1)", err)
			h.fileID = 1
		}
	}

	log.Printf("[Handle] Created for %s (fileID=%d, size=%d)", cfg.Path, h.fileID, cfg.Size)

	// Start adaptive predictive pump
	pump := NewAdaptivePump(h)
	pump.Start()
	h.pump = pump

	return h
}

// Read serves data from cache or fetches from torrent via FetchBlock.
func (h *MkvHandle) Read(buf []byte, offset int64) (int, error) {
	atomic.StoreInt64(&h.lastActivityTime, time.Now().UnixNano())
	h.mu.Lock()
	h.lastOff = offset
	h.mu.Unlock()

	// Feed player position to the adaptive pump
	if h.pump != nil {
		h.pump.RecordRead(offset)
	}

	// 1. Try cache (fast path — served by pump)
	n := h.raCache.CopyTo(h.path, buf, offset)
	if n > 0 {
		return n, nil
	}

	// 2. Cache miss — fetch via FetchBlock with retry
	return h.fetchWithRetry(buf, offset)
}

func (h *MkvHandle) fetchWithRetry(buf []byte, offset int64) (int, error) {
	if h.client == nil {
		return 0, fmt.Errorf("no client")
	}

	deadline := time.Now().Add(fetchMaxWaitS * time.Second)
	var lastErr error

	for attempt := 0; time.Now().Before(deadline); attempt++ {
		if h.isClosed() {
			return 0, fmt.Errorf("handle closed")
		}

		n, err := h.client.FetchBlock(h.hash, h.fileID, offset, buf)
		if err == nil && n > 0 {
			h.raCache.Put(h.path, offset, buf[:n])
			return n, nil
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(fetchRetryDelay)
	}
	return 0, fmt.Errorf("fetch timed out: %w", lastErr)
}

// Release is a no-op — handle stays alive across VLC's probing cycles.
func (h *MkvHandle) Release() {
	log.Printf("[Handle] Released: %s (lastOff=%d, pump=%v)",
		h.path, atomic.LoadInt64(&h.lastOff), h.pump != nil)
}

// Close fully shuts down the handle and its pump.
func (h *MkvHandle) Close() {
	if !h.closed.CompareAndSwap(false, true) {
		return // already closed
	}
	h.mu.Lock()
	if h.pump != nil {
		h.pump.Stop()
		h.pump = nil
	}
	h.mu.Unlock()
}

func (h *MkvHandle) isClosed() bool {
	return h.closed.Load()
}

func (h *MkvHandle) GetLastOff() int64 {
	return atomic.LoadInt64(&h.lastOff)
}

// GetHash returns the torrent info-hash for this handle.
func (h *MkvHandle) GetHash() string {
	return h.hash
}

func (h *MkvHandle) IsActive(timeout time.Duration) bool {
	last := atomic.LoadInt64(&h.lastActivityTime)
	return time.Since(time.Unix(0, last)) < timeout
}
