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

// ReadAheadCache is a 32-shard concurrent LRU cache for streaming data.
type ReadAheadCache struct {
	shards      [numShards]*raShard
	used        int64 // atomic, total bytes cached
	pool        chan []byte
	chunkSize   int64
	budget      int64
	muContext   sync.Mutex
	activePath  string
	sessionID   int64
	pieceLens   sync.Map // path -> int64 (adaptive chunk size)
}

type raShard struct {
	buffers map[string]*raBuffer
	order   []string
	mu      sync.RWMutex
	total   int64
}

type raBuffer struct {
	start, end int64
	data       []byte
	lastAccess int64 // atomic, nanosecond timestamp
	sessionID  int64
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
			buffers: make(map[string]*raBuffer),
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

func chunkKey(path string, offset int64) string {
	return path + ":" + itoa(offset)
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

	// Get a buffer (recycle or allocate)
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

	var kept []string
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
	select {
	case buf := <-c.pool:
		if cap(buf) >= size {
			return buf[:size]
		}
		return make([]byte, size)
	default:
		return make([]byte, size)
	}
}

func (c *ReadAheadCache) recycle(buf []byte) {
	if cap(buf) == int(c.chunkSize) {
		select {
		case c.pool <- buf[:0]:
		default:
		}
	}
}

// itoa is a fast int64 to string conversion for cache keys.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 20)
	for n > 0 {
		b = append(b, byte('0'+n%10))
		n /= 10
	}
	// Reverse
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return string(b)
}
