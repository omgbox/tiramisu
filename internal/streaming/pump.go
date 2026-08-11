package streaming

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// NativeReader abstracts the torrent streaming reader.
type NativeReader interface {
	ReadAt(p []byte, off int64) (n int, err error)
	Interrupt()
	Close() error
}

// NativeClient abstracts the torrent engine bridge.
type NativeClient interface {
	Wake(magnet string, fileIdx int) error
	NewStreamReader(hash string, fileID int, totalSize int64) NativeReader
	FetchBlock(hash string, fileID int, offset int64, buf []byte) (int, error)
	RemoveTorrent(hash string) error
}

// MasterSemaphore controls concurrency for data operations.
type MasterSemaphore struct {
	sem chan struct{}
}

func NewMasterSemaphore(limit int) *MasterSemaphore {
	return &MasterSemaphore{sem: make(chan struct{}, limit)}
}

func (s *MasterSemaphore) Acquire() bool {
	select {
	case s.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *MasterSemaphore) Release() {
	<-s.sem
}

func (s *MasterSemaphore) Len() int {
	return len(s.sem)
}

// NativePumpState tracks a shared pump across multiple handles for the same file.
type NativePumpState struct {
	cancel   context.CancelFunc
	reader   NativeReader
	path     string
	refCount int32
	playerOff int64 // last known player position
}

// NativePump reads continuously from the NativeReader and fills raCache.
type NativePump struct {
	hash       string
	fileID     int
	path       string
	size       int64
	reader     NativeReader
	raCache    *ReadAheadCache
	semaphore  *MasterSemaphore
	state      *NativePumpState
	cancel     context.CancelFunc
	slotAcquired bool

	// Player tracking
	lastKnownPlayerOff int64
	lastRevival        time.Time
}

// NewNativePump creates a new pump.
func NewNativePump(hash string, fileID int, path string, size int64,
	reader NativeReader, raCache *ReadAheadCache, semaphore *MasterSemaphore) *NativePump {
	return &NativePump{
		hash:       hash,
		fileID:     fileID,
		path:       path,
		size:       size,
		reader:     reader,
		raCache:    raCache,
		semaphore:  semaphore,
		lastRevival: time.Now().Add(-time.Hour),
	}
}

// Start begins the background pump goroutine.
func (p *NativePump) Start(startOffset int64) {
	if !p.semaphore.Acquire() {
		log.Printf("[Pump] Semaphore full, %s using fallback mode", p.path)
		return
	}
	p.slotAcquired = true

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	p.state = &NativePumpState{
		cancel:   cancel,
		reader:   p.reader,
		path:     p.path,
		refCount: 1,
	}

	go p.run(ctx, startOffset)
}

// UpdatePlayerOff updates the pump's knowledge of the player position.
func (p *NativePump) UpdatePlayerOff(off int64) {
	atomic.StoreInt64(&p.lastKnownPlayerOff, off)
	if p.state != nil {
		atomic.StoreInt64(&p.state.playerOff, off)
	}
}

// Stop cancels the pump goroutine.
func (p *NativePump) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *NativePump) run(ctx context.Context, startOffset int64) {
	defer func() {
		if p.slotAcquired {
			p.semaphore.Release()
			p.slotAcquired = false
		}
		p.reader.Close()
		log.Printf("[Pump] Ended: %s", p.path)
	}()

	chunkSize := p.raCache.ChunkSize(p.path)
	offset := (startOffset / chunkSize) * chunkSize
	pumpedBytes := int64(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Skip if already cached
		if p.raCache.Covered(p.path, offset, chunkSize) {
			offset += chunkSize
			if offset >= p.size && p.size > 0 {
				offset = 0 // wrap for seeding
			}
			continue
		}

		// Read chunk from torrent
		buf := make([]byte, chunkSize)
		n, err := p.reader.ReadAt(buf, offset)
		if err != nil {
			if err == context.Canceled || err == io.ErrClosedPipe {
				return
			}
			// Interrupted (seek) — re-anchor
			playerOff := atomic.LoadInt64(&p.lastKnownPlayerOff)
			if playerOff > 0 {
				offset = (playerOff / chunkSize) * chunkSize
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if n > 0 {
			p.raCache.Put(p.path, offset, buf[:n])
			pumpedBytes += int64(n)
			offset += int64(n)
		}

		if offset >= p.size && p.size > 0 {
			offset = 0 // wrap for seeding
		}

		// Throttle after 64MB grace period
		if pumpedBytes > 64*1024*1024 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// ActivePumps manages all active pumps by path.
type ActivePumps struct {
	mu      sync.Mutex
	pumps   map[string]*NativePump
	readers map[string]*NativePumpState
}

func NewActivePumps() *ActivePumps {
	return &ActivePumps{
		pumps:   make(map[string]*NativePump),
		readers: make(map[string]*NativePumpState),
	}
}

// GetOrCreate returns the existing pump for a path, or creates a new one.
func (ap *ActivePumps) GetOrCreate(path string, create func() *NativePump) *NativePump {
	ap.mu.Lock()
	defer ap.mu.Unlock()

	if p, ok := ap.pumps[path]; ok {
		return p
	}

	p := create()
	ap.pumps[path] = p
	return p
}

// Remove removes a pump for a path.
func (ap *ActivePumps) Remove(path string) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	delete(ap.pumps, path)
}

// Get returns the pump for a path, or nil.
func (ap *ActivePumps) Get(path string) *NativePump {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	return ap.pumps[path]
}

// fetchBlockWithRetry fetches a single chunk with retries.
func fetchBlockWithRetry(client NativeClient, hash string, fileID int,
	offset int64, buf []byte, maxRetries int) (int, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		n, err := client.FetchBlock(hash, fileID, offset, buf)
		if err == nil {
			return n, nil
		}
		lastErr = err
		if attempt < maxRetries-1 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return 0, fmt.Errorf("fetch block failed after %d retries: %w", maxRetries, lastErr)
}
