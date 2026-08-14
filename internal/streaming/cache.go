package streaming

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	numShards        = 32
	shardMask        = numShards - 1
	defaultChunkSize = 16 * 1024 * 1024 // 16MB
	evictionAgeSec   = 120              // evict entries older than 120s
)

// cacheKey is a composite key avoiding string allocation on the hot path.
type cacheKey struct {
	offset int64  // 8 bytes — largest first
	path   string // 16 bytes (pointer + len)
}

// ReadAheadCache is a 32-shard concurrent LRU cache for streaming data.
type ReadAheadCache struct {
	shards      [numShards]*raShard
	budget      int64         // 8 bytes
	chunkSize   int64         // 8 bytes
	used        int64         // atomic, 8 bytes
	pool        chan []byte
	muContext   sync.Mutex
	sessionID   int64
	activePath  string
	pieceLens   sync.Map // path -> int64 (adaptive chunk size)
}

type raShard struct {
	mu      sync.RWMutex
	buffers map[cacheKey]*raBuffer
	order   []cacheKey
	total   int64
}

// raBuffer fields ordered: largest→smallest for alignment.
// int64 fields first, then pointer, then int64, then int32.
type raBuffer struct {
	data       []byte // 24 bytes (pointer + len + cap)
	start      int64  // 8 bytes
	end        int64  // 8 bytes
	lastAccess int64  // 8 bytes — atomic, nanosecond timestamp
	sessionID  int64  // 8 bytes
}

// NewReadAheadCache creates a new cache with the given budget in bytes.
func NewReadAheadCache(budget int64, chunkSize int64) *ReadAheadCache {
	c := &ReadAheadCache{
		chunkSize: chunkSize,
		budget:    budget,
	}
	if c.chunkSize <= 0 {
		c.chunkSize = defaultChunkSize
	}
	for i := 0; i < numShards; i++ {
		c.shards[i] = &raShard{
			buffers: make(map[cacheKey]*raBuffer),
		}
	}
	// Pool of recycled buffers
	poolSize := int(budget / c.chunkSize * 2)
	if poolSize < 16 {
		poolSize = 16
	}
	if poolSize > 64 {
		poolSize = 64
	}
	c.pool = make(chan []byte, poolSize)
	return c
}

func (c *ReadAheadCache) shardFor(path string) *raShard {
	h := xxhash.Sum64String(path)
	return c.shards[h&shardMask]
}

func chunkKey(path string, offset int64) cacheKey {
	return cacheKey{path: path, offset: offset}
}

// SetPieceLen stores the adaptive chunk size for a path.
func (c *ReadAheadCache) SetPieceLen(path string, pieceLen int64) {
	if pieceLen > 0 {
		c.pieceLens.Store(path, pieceLen)
	}
}

// ChunkSize returns the per-path chunk size, falling back to default.
func (c *ReadAheadCache) ChunkSize(path string) int64 {
	if v, ok := c.pieceLens.Load(path); ok {
		if sz := v.(int64); sz > 0 {
			return sz
		}
	}
	return c.chunkSize
}

// SwitchContext is called on Open to invalidate stale data from the previous file.
func (c *ReadAheadCache) SwitchContext(path string) {
	c.muContext.Lock()
	if c.activePath != path {
		c.activePath = path
		c.sessionID++
	}
	c.muContext.Unlock()
}

// Put writes data into the cache at the given path and offset.
func (c *ReadAheadCache) Put(path string, offset int64, data []byte) {
	key := chunkKey(path, offset)
	shard := c.shardFor(path)

	// Get a buffer (recycle or allocate) and copy data into it
	buf := c.getBuffer(len(data))
	copy(buf, data[:len(data)])

	shard.mu.Lock()
	// If overwriting, recycle old buffer
	if old, ok := shard.buffers[key]; ok {
		c.recycle(old.data)
		shard.total -= (old.end - old.start)
		atomic.AddInt64(&c.used, -(old.end - old.start))
	}

	shard.buffers[key] = &raBuffer{
		start:      offset,
		end:        offset + int64(len(data)),
		data:       buf,
		lastAccess: time.Now().UnixNano(),
		sessionID:  c.sessionID,
	}
	shard.total += int64(len(data))
	atomic.AddInt64(&c.used, int64(len(data)))
	shard.order = append(shard.order, key)
	shard.mu.Unlock()

	// Evict if over budget
	if atomic.LoadInt64(&c.used) > c.budget {
		c.evictShard(shard)
	}
}

// PutOwned stores data without copying — caller must not use buf after this call.
// Zero-copy path for pump fetchChunk buffers.
func (c *ReadAheadCache) PutOwned(path string, offset int64, buf []byte) {
	key := chunkKey(path, offset)
	shard := c.shardFor(path)
	n := len(buf)

	shard.mu.Lock()
	// If overwriting, recycle old buffer
	if old, ok := shard.buffers[key]; ok {
		c.recycle(old.data)
		shard.total -= (old.end - old.start)
		atomic.AddInt64(&c.used, -(old.end - old.start))
	}

	shard.buffers[key] = &raBuffer{
		start:      offset,
		end:        offset + int64(n),
		data:       buf,
		lastAccess: time.Now().UnixNano(),
		sessionID:  c.sessionID,
	}
	shard.total += int64(n)
	atomic.AddInt64(&c.used, int64(n))
	shard.order = append(shard.order, key)
	shard.mu.Unlock()

	// Evict if over budget
	if atomic.LoadInt64(&c.used) > c.budget {
		c.evictShard(shard)
	}
}

// CopyTo copies cached data into buf at the given offset. Returns bytes copied.
func (c *ReadAheadCache) CopyTo(path string, buf []byte, offset int64) int {
	key := chunkKey(path, offset)
	shard := c.shardFor(path)

	shard.mu.RLock()
	b, ok := shard.buffers[key]
	shard.mu.RUnlock()

	if !ok {
		return 0
	}

	n := copy(buf, b.data)
	atomic.StoreInt64(&b.lastAccess, time.Now().UnixNano())
	return n
}

// Get returns a copy of cached data. Caller must NOT modify the returned slice.
func (c *ReadAheadCache) Get(path string, offset, size int64) ([]byte, int) {
	key := chunkKey(path, offset)
	shard := c.shardFor(path)

	shard.mu.RLock()
	b, ok := shard.buffers[key]
	shard.mu.RUnlock()

	if !ok {
		return nil, 0
	}

	n := int(size)
	if n > len(b.data) {
		n = len(b.data)
	}
	out := make([]byte, n)
	copy(out, b.data[:n])
	atomic.StoreInt64(&b.lastAccess, time.Now().UnixNano())
	return out, n
}

// Covered checks if a byte range is already cached (no allocation).
func (c *ReadAheadCache) Covered(path string, offset, size int64) bool {
	key := chunkKey(path, offset)
	shard := c.shardFor(path)

	shard.mu.RLock()
	b, ok := shard.buffers[key]
	shard.mu.RUnlock()

	return ok && (b.end-b.start) >= size
}

// MaxCachedOffset returns the highest cached byte end for a path.
func (c *ReadAheadCache) MaxCachedOffset(path string) int64 {
	var maxOff int64
	shard := c.shardFor(path)

	shard.mu.RLock()
	for _, b := range shard.buffers {
		if b.end > maxOff {
			maxOff = b.end
		}
	}
	shard.mu.RUnlock()
	return maxOff
}

// Stats returns cache statistics.
func (c *ReadAheadCache) Stats() (used int64, entries int) {
	used = atomic.LoadInt64(&c.used)
	for _, shard := range c.shards {
		shard.mu.RLock()
		entries += len(shard.buffers)
		shard.mu.RUnlock()
	}
	return
}

func (c *ReadAheadCache) evictShard(shard *raShard) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	now := time.Now().UnixNano()
	ageLimit := int64(evictionAgeSec) * int64(time.Second)

	kept := make([]cacheKey, 0, len(shard.order))
	for _, key := range shard.order {
		b, ok := shard.buffers[key]
		if !ok {
			continue
		}
		elapsed := now - atomic.LoadInt64(&b.lastAccess)
		if elapsed > ageLimit && b.sessionID != c.sessionID {
			// Evict stale entry
			c.recycle(b.data)
			shard.total -= (b.end - b.start)
			atomic.AddInt64(&c.used, -(b.end - b.start))
			delete(shard.buffers, key)
		} else {
			kept = append(kept, key)
		}
	}
	shard.order = kept
}

func (c *ReadAheadCache) getBuffer(size int) []byte {
	// Try to reuse a pooled buffer (any size >= requested)
	select {
	case buf := <-c.pool:
		if cap(buf) >= size {
			return buf[:size]
		}
		// Too small — put it back and allocate fresh
		select {
		case c.pool <- buf[:0]:
		default:
		}
		return make([]byte, size)
	default:
		return make([]byte, size)
	}
}

func (c *ReadAheadCache) recycle(buf []byte) {
	// Recycle any buffer >= 128KB (adaptive chunk sizing produces variable sizes)
	if cap(buf) >= 128*1024 {
		select {
		case c.pool <- buf[:0]:
		default:
		}
	}
}
