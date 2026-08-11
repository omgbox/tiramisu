package streaming

import (
	"sync"
	"sync/atomic"
)

// ReadBufferPool manages recycled read buffers.
type ReadBufferPool struct {
	pool sync.Pool
	size int
}

// NewReadBufferPool creates a pool of buffers with the given size.
func NewReadBufferPool(size int) *ReadBufferPool {
	return &ReadBufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		},
		size: size,
	}
}

// Get returns a buffer from the pool.
func (p *ReadBufferPool) Get() *[]byte {
	return p.pool.Get().(*[]byte)
}

// Put returns a buffer to the pool.
func (p *ReadBufferPool) Put(buf *[]byte) {
	if buf != nil && cap(*buf) >= p.size {
		*buf = (*buf)[:p.size]
		p.pool.Put(buf)
	}
}

// FetchFlight tracks an in-flight FetchBlock call for deduplication.
type FetchFlight struct {
	done chan struct{}
	n    int
	err  error
}

// FetchFlightDedup deduplicates concurrent FetchBlock calls to the same chunk.
type FetchFlightDedup struct {
	flights sync.Map // key: "path:offset" -> *FetchFlight
	counter atomic.Int64
}

// Start tries to start a fetch. Returns (flight, isLeader).
// If isLeader is true, the caller must call flight.Complete() when done.
// If isLeader is false, the caller should wait on flight.done.
func (d *FetchFlightDedup) Start(path string, offset int64) (*FetchFlight, bool) {
	key := path + ":" + itoa(offset)

	// Check if there's already a flight in progress
	if existing, loaded := d.flights.Load(key); loaded {
		return existing.(*FetchFlight), false
	}

	// Try to create a new flight
	flight := &FetchFlight{
		done: make(chan struct{}),
	}
	if actual, loaded := d.flights.LoadOrStore(key, flight); loaded {
		// Lost the race — someone else created it
		return actual.(*FetchFlight), false
	}

	d.counter.Add(1)
	return flight, true
}

// Complete marks a flight as done and removes it from the map.
func (d *FetchFlightDedup) Complete(path string, offset int64, n int, err error) {
	key := path + ":" + itoa(offset)
	if v, ok := d.flights.LoadAndDelete(key); ok {
		flight := v.(*FetchFlight)
		flight.n = n
		flight.err = err
		close(flight.done)
	}
}

// ActiveCount returns the number of in-flight fetches.
func (d *FetchFlightDedup) ActiveCount() int {
	return int(d.counter.Load())
}
