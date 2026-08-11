package streaming

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// MkvHandle represents an open file handle for a virtual .mkv file.
type MkvHandle struct {
	path      string
	url       string
	magnet    string
	size      int64
	hash      string
	fileID    int

	lastOff          int64 // atomic, player read offset
	lastLen          int
	lastTime         time.Time
	lastActivityTime time.Time

	mu       sync.Mutex
	pump     *NativePump
	reader   NativeReader
	client   NativeClient
	raCache  *ReadAheadCache
	hasSlot  bool
	closed   bool
}

// HandleConfig holds parameters for creating a new handle.
type HandleConfig struct {
	Path   string
	URL    string
	Magnet string
	Size   int64
	Hash   string
	FileID int
	Client NativeClient
	Cache  *ReadAheadCache
}

// NewHandle creates a new MkvHandle and initializes the persistent NativeReader.
func NewHandle(cfg HandleConfig) *MkvHandle {
	h := &MkvHandle{
		path:             cfg.Path,
		url:              cfg.URL,
		magnet:           cfg.Magnet,
		size:             cfg.Size,
		hash:             cfg.Hash,
		fileID:           cfg.FileID,
		client:           cfg.Client,
		raCache:          cfg.Cache,
		lastActivityTime: time.Now(),
		lastTime:         time.Now(),
		lastOff:          -1,
	}

	// Wake torrent engine
	if cfg.Client != nil && cfg.Magnet != "" {
		if err := cfg.Client.Wake(cfg.Magnet, cfg.FileID); err != nil {
			log.Printf("[Handle] Wake failed for %s: %v", cfg.Path, err)
		} else {
			// Create persistent reader after Wake succeeds
			h.reader = cfg.Client.NewStreamReader(cfg.Hash, cfg.FileID, cfg.Size)
			log.Printf("[Handle] Reader created for %s (size=%d)", cfg.Path, cfg.Size)
		}
	}

	return h
}

// Read serves data from cache or fetches from torrent via persistent reader.
func (h *MkvHandle) Read(buf []byte, offset int64) (int, error) {
	h.lastActivityTime = time.Now()
	atomic.StoreInt64(&h.lastOff, offset)
	h.mu.Lock()
	h.lastOff = offset
	h.lastLen = len(buf)
	h.lastTime = time.Now()
	h.mu.Unlock()

	// Try cache first
	n := h.raCache.CopyTo(h.path, buf, offset)
	if n > 0 {
		return n, nil
	}

	// Cache miss — use persistent NativeReader
	if h.reader == nil {
		return 0, ErrNoReader
	}

	n, err := h.reader.ReadAt(buf, offset)
	if err != nil {
		return 0, err
	}

	if n > 0 {
		h.raCache.Put(h.path, offset, buf[:n])
	}

	return n, nil
}

// Release closes the handle and cleans up resources.
func (h *MkvHandle) Release() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}
	h.closed = true

	// Close persistent reader
	if h.reader != nil {
		h.reader.Close()
		h.reader = nil
	}

	// Save player position to pump state
	if h.pump != nil && h.pump.state != nil {
		atomic.StoreInt64(&h.pump.state.playerOff, h.lastOff)
	}

	log.Printf("[Handle] Released: %s (lastOff=%d)", h.path, h.lastOff)
}

// GetLastOff returns the last read offset.
func (h *MkvHandle) GetLastOff() int64 {
	return atomic.LoadInt64(&h.lastOff)
}

// IsActive returns true if the handle has been read recently.
func (h *MkvHandle) IsActive(timeout time.Duration) bool {
	return time.Since(h.lastActivityTime) < timeout
}

// Errors
var (
	ErrNoClient = fmt.Errorf("no torrent client configured")
	ErrNoReader = fmt.Errorf("no torrent reader (Wake may have failed)")
)
