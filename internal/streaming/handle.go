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
// The pump starts immediately; torrent init (Wake + FindFileID) runs async
// so FUSE Open returns instantly and doesn't block Explorer/VLC.
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

	// Start pump immediately with fileID=0 (fast path, non-blocking).
	// Pump fetches will fail initially but retry; once async init completes
	// the fileID is set and subsequent fetches succeed.
	pump := NewAdaptivePump(h)
	pump.Start()
	h.pump = pump

	log.Printf("[Handle] Created for %s (size=%d, async init)", cfg.Path, cfg.Size)

	// Async torrent init: Wake + FindFileID in background.
	// This unblocks FUSE Open so Explorer/VLC don't freeze.
	go h.asyncInit(cfg)

	return h
}

// asyncInit wakes the torrent engine and resolves the file ID in the background.
// The pump retries fetches until this completes, at which point fileID is set.
func (h *MkvHandle) asyncInit(cfg HandleConfig) {
	if err := cfg.Client.Wake(cfg.Magnet, 0); err != nil {
		log.Printf("[Handle] Wake failed for %s: %v", cfg.Path, err)
		return
	}

	if cfg.FilePath != "" {
		if fid, err := cfg.Client.FindFileID(cfg.Hash, cfg.FilePath); err == nil {
			h.fileID = fid
			log.Printf("[Handle] Async init complete: %s fileID=%d", cfg.Path, fid)
		} else {
			log.Printf("[Handle] FindFileID failed: %v (fallback 1)", err)
			h.fileID = 1
		}
	}
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
