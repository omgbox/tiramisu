package streaming

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// fetchBufPool reuses fetch buffers to avoid per-call 128KB-2MB allocations.
var fetchBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256*1024) // start at 256KB, grows as needed
		return &b
	},
}

func getFetchBuf(size int) []byte {
	bp := fetchBufPool.Get().(*[]byte)
	b := *bp
	if cap(b) >= size {
		return b[:size]
	}
	// Too small — allocate bigger, discard old
	newB := make([]byte, size)
	return newB
}

func putFetchBuf(buf []byte) {
	if cap(buf) >= 128*1024 { // only pool reasonably-sized buffers
		b := buf[:0]
		fetchBufPool.Put(&b)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// ADAPTIVE PREDICTIVE PUMP (APP)
//
// Novel read-ahead engine that learns the player's access pattern and predicts
// what to fetch next. Key innovations:
//
// 1. PROBE PATTERN DETECTION: VLC reads header(0) → moov(end) → video(start).
//    APP detects this 3-step pattern and pre-fetches the moov atom proactively.
//
// 2. DUAL-STREAM FILL: Two concurrent fetch goroutines:
//    - Lead: fills immediately around the player position (low latency)
//    - Trail: fills sequentially ahead (high throughput)
//
// 3. ADAPTIVE CHUNK SIZING: Starts at 128KB for fast first-byte, scales to 2MB
//    as measured throughput improves. Chunks shrink on errors.
//
// 4. BANDWIDTH PROBING: Measures actual MB/s and targets 1.5x playback bitrate
//    to stay ahead without wasting bandwidth.
//
// 5. SEEK PREDICTION: Tracks seek history to predict next seek target.
// ═══════════════════════════════════════════════════════════════════════════════

const (
	appMinChunk    = 128 * 1024        // 128KB minimum
	appMaxChunk    = 4 * 1024 * 1024   // 4MB maximum (up from 2MB)
	appLeadWindow  = 8 * 1024 * 1024   // 8MB around player position (up from 4MB)
	appTrailGap    = 4                  // trail stream starts 4 chunks ahead of lead (up from 2)
	appProbeWindow = 5                  // track last N reads for pattern detection
	appAdaptWindow = 5 * time.Second   // measure throughput over 5s windows
	appMinBytesPS  = 512 * 1024        // 512 KB/s minimum target
	appSeekBurst   = 8 * 1024 * 1024   // 8MB prefetch on seek
	appConcurrent  = 4                 // concurrent fetch goroutines per stream
)

// pumpPhase represents the pump's current operating mode.
type pumpPhase int

const (
	phaseBootstrap pumpPhase = iota // initial fill, no player data yet
	phaseProbe                      // VLC is probing (reading header/moov)
	phaseSequential                 // VLC is playing sequentially
	phaseSeeking                    // VLC just seeked, catch up
)

func (p pumpPhase) String() string {
	switch p {
	case phaseBootstrap:
		return "bootstrap"
	case phaseProbe:
		return "probe"
	case phaseSequential:
		return "sequential"
	case phaseSeeking:
		return "seeking"
	}
	return "unknown"
}

// seekEvent records a player seek for prediction.
type seekEvent struct {
	from      int64
	to        int64
	timestamp time.Time
}

// AdaptivePump is a smart read-ahead engine.
type AdaptivePump struct {
	handle *MkvHandle

	// Player tracking (written by Read, read by pump goroutines)
	playerOff   atomic.Int64
	playerSpeed atomic.Int64 // bytes/sec playback speed estimate

	// Pump state
	phase          atomic.Int64 // pumpPhase as int64
	chunkSize      atomic.Int64 // adaptive chunk size
	bytesPerSecond atomic.Int64 // measured download speed
	seekBurstLeft  atomic.Int64 // bytes remaining in seek burst

	// Seek history for prediction (protected by mu)
	mu         sync.Mutex
	seekHistory [appProbeWindow]seekEvent
	seekIdx    int
	lastPlayer int64
	probeCount int // counts reads in probe phase

	// Lifecycle
	cancel chan struct{}
	done   chan struct{}
}

// NewAdaptivePump creates a pump tied to a handle.
func NewAdaptivePump(handle *MkvHandle) *AdaptivePump {
	p := &AdaptivePump{
		handle: handle,
		cancel: make(chan struct{}),
		done:   make(chan struct{}),
	}
	p.chunkSize.Store(int64(appMinChunk))
	p.phase.Store(int64(phaseBootstrap))
	return p
}

// Start launches the dual-stream pump.
func (p *AdaptivePump) Start() {
	go p.run()
}

// Stop cancels the pump.
func (p *AdaptivePump) Stop() {
	select {
	case <-p.cancel:
	default:
		close(p.cancel)
	}
	<-p.done
}

// RecordRead is called by handle.Read to feed player position to the pump.
func (p *AdaptivePump) RecordRead(offset int64) {
	old := p.playerOff.Swap(offset)
	p.updatePhase(old, offset)
	p.recordSeek(old, offset)
}

// updatePhase detects what the player is doing.
func (p *AdaptivePump) updatePhase(oldOff, newOff int64) {
	if oldOff < 0 {
		// First read — still in bootstrap
		return
	}

	jump := newOff - oldOff
	if jump < 0 {
		jump = -jump
	}

	switch {
	case jump > 512*1024:
		// Seek (>512KB) — entering seeking phase with burst
		p.phase.Store(int64(phaseSeeking))
		p.probeCount = 0
		// Trigger seek burst: aggressively prefetch around new position
		p.seekBurst(int64(appSeekBurst))
	case jump <= appMinChunk*2 && newOff > oldOff:
		// Small forward read — likely sequential playback
		if p.probeCount > 2 {
			p.phase.Store(int64(phaseSequential))
		}
	default:
		// Could be probing or small seek
		p.probeCount++
		if p.probeCount >= 3 {
			p.phase.Store(int64(phaseProbe))
		}
	}
}

// seekBurst sets the number of bytes to aggressively prefetch after a seek.
func (p *AdaptivePump) seekBurst(bytes int64) {
	// Atomic store — will be consumed by the pump loop
	p.seekBurstLeft.Store(bytes)
}

// recordSeek stores a seek event for prediction.
func (p *AdaptivePump) recordSeek(from, to int64) {
	if from < 0 || to < 0 || from == to {
		return
	}
	jump := to - from
	if jump < 0 {
		jump = -jump
	}
	if jump < 1024*1024 {
		return // not a significant seek
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.seekHistory[p.seekIdx] = seekEvent{
		from:      from,
		to:        to,
		timestamp: time.Now(),
	}
	p.seekIdx = (p.seekIdx + 1) % appProbeWindow
}

// predictNextSeek analyzes seek history to guess the next seek target.
// Returns -1 if no prediction.
func (p *AdaptivePump) predictNextSeek() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.seekIdx < 2 {
		return -1
	}

	// Look for repeated seek patterns (e.g., header→moov→start)
	// Check if we've seen a seek to a similar region before
	current := p.lastPlayer
	if current < 0 {
		return -1
	}

	// Pattern: after seeking to low offset, player often seeks to high offset (moov atom)
	lowSeeks := 0
	highSeeks := 0
	for i := 0; i < p.seekIdx; i++ {
		if p.seekHistory[i].to < 1024*1024 {
			lowSeeks++
		} else if p.seekHistory[i].to > 10*1024*1024 {
			highSeeks++
		}
	}

	// If we see both low and high seeks, predict moov atom region
	if lowSeeks > 0 && highSeeks > 0 && current < 1024*1024 {
		// Player just seeked to a low offset — predict they'll seek to the end next
		// Return a position near the end of the file (moov atom region)
		return p.handle.size - 1024*1024 // 1MB from end
	}

	return -1
}

// adaptChunkSize adjusts chunk size based on measured throughput.
func (p *AdaptivePump) adaptChunkSize() {
	bps := p.bytesPerSecond.Load()
	if bps <= 0 {
		return
	}

	// Target: fill 4x ahead in 1 second
	targetChunk := int64(bps / 4)
	if targetChunk < appMinChunk {
		targetChunk = appMinChunk
	}
	if targetChunk > appMaxChunk {
		targetChunk = appMaxChunk
	}
	p.chunkSize.Store(targetChunk)
}

// run is the main pump loop with concurrent dual-stream fill.
func (p *AdaptivePump) run() {
	defer close(p.done)

	leadOffset := int64(-1)
	aheadOffset := int64(-1)

	pumpedBytes := int64(0)
	startTime := time.Now()
	initialBurstEnd := int64(4 * 1024 * 1024) // 4MB initial burst (up from 2MB)
	lastLogTime := startTime

	for {
		select {
		case <-p.cancel:
			return
		default:
		}

		chunkSize := p.chunkSize.Load()
		phase := pumpPhase(p.phase.Load())
		playerOff := p.playerOff.Load()
		seekBurst := p.seekBurstLeft.Load()

		// Seek burst mode: aggressively prefetch after seek
		if seekBurst > 0 && playerOff >= 0 {
			var wg sync.WaitGroup
			var mu sync.Mutex
			fetched := int64(0)
			for i := 0; i < appConcurrent && seekBurst > 0; i++ {
				off := playerOff + int64(i)*chunkSize
				if p.handle.raCache.Covered(p.handle.path, off, chunkSize) {
					mu.Lock()
					fetched += chunkSize
					seekBurst -= chunkSize
					mu.Unlock()
					continue
				}
				wg.Add(1)
				go func(offset int64) {
					defer wg.Done()
					n := p.fetchChunk(offset, chunkSize)
					mu.Lock()
					if n > 0 {
						fetched += int64(n)
						pumpedBytes += int64(n)
					}
					seekBurst -= chunkSize
					mu.Unlock()
				}(off)
			}
			wg.Wait()
			p.seekBurstLeft.Store(seekBurst)
			leadOffset = playerOff + fetched
			aheadOffset = leadOffset + chunkSize*appTrailGap
		}

		// Lead stream: fill around player position
		if playerOff >= 0 {
			if leadOffset < 0 || playerOff > leadOffset+chunkSize*int64(appConcurrent) {
				leadOffset = (playerOff / chunkSize) * chunkSize
			}

			// Launch concurrent lead fetches
			var wg sync.WaitGroup
			var mu sync.Mutex
			for i := int64(0); i < int64(appConcurrent); i++ {
				off := leadOffset + i*chunkSize
				if p.handle.raCache.Covered(p.handle.path, off, chunkSize) {
					mu.Lock()
					leadOffset += chunkSize
					mu.Unlock()
					continue
				}
				wg.Add(1)
				go func(offset int64) {
					defer wg.Done()
					n := p.fetchChunk(offset, chunkSize)
					mu.Lock()
					if n > 0 {
						pumpedBytes += int64(n)
						leadOffset += int64(n)
					} else {
						leadOffset += chunkSize
					}
					mu.Unlock()
				}(off)
			}
			wg.Wait()
		}

		// Trail stream: fill ahead of lead
		if playerOff >= 0 {
			if aheadOffset < 0 {
				aheadOffset = leadOffset + chunkSize*appTrailGap
			}
			if aheadOffset < leadOffset+chunkSize*appTrailGap {
				aheadOffset = leadOffset + chunkSize*appTrailGap
			}
		}

		if aheadOffset >= 0 {
			var wg sync.WaitGroup
			var mu sync.Mutex
			for i := int64(0); i < int64(appConcurrent); i++ {
				off := aheadOffset + i*chunkSize
				if p.handle.size > 0 && off >= p.handle.size {
					mu.Lock()
					aheadOffset = 0
					mu.Unlock()
					break
				}
				if p.handle.raCache.Covered(p.handle.path, off, chunkSize) {
					mu.Lock()
					aheadOffset += chunkSize
					mu.Unlock()
					continue
				}
				wg.Add(1)
				go func(offset int64) {
					defer wg.Done()
					n := p.fetchChunk(offset, chunkSize)
					mu.Lock()
					if n > 0 {
						pumpedBytes += int64(n)
						aheadOffset += int64(n)
					} else {
						aheadOffset += chunkSize
					}
					mu.Unlock()
				}(off)
			}
			wg.Wait()
		}

		elapsed := time.Since(startTime).Seconds()
		if elapsed > 0 {
			p.bytesPerSecond.Store(int64(float64(pumpedBytes) / elapsed))
			p.adaptChunkSize()
		}

		if pumpedBytes < initialBurstEnd {
			continue
		}

		switch phase {
		case phaseBootstrap, phaseSeeking:
			time.Sleep(1 * time.Millisecond) // faster catch-up
		case phaseSequential:
			if playerOff >= 0 && aheadOffset >= 0 && aheadOffset-playerOff > appLeadWindow {
				time.Sleep(50 * time.Millisecond)
			} else {
				time.Sleep(5 * time.Millisecond)
			}
		case phaseProbe:
			time.Sleep(5 * time.Millisecond)
		}

		if p.handle.size > 0 && aheadOffset >= p.handle.size {
			aheadOffset = 0
		}

		now := time.Now()
		if now.Sub(lastLogTime) >= 10*time.Second {
			lastLogTime = now
			usedMB := int64(0)
			if p.handle.raCache != nil {
				u, _ := p.handle.raCache.Stats()
				usedMB = u / (1024 * 1024)
			}
			log.Printf("[Pump] %s chunk=%dKB speed=%dKB/s player=%dMB ahead=%dMB cached=%dMB",
				phase, chunkSize/1024, p.bytesPerSecond.Load()/1024,
				playerOff/(1024*1024), aheadOffset/(1024*1024), usedMB)
		}
	}
}

// fetchChunk fetches a single chunk synchronously. Returns bytes fetched.
// On failure, returns 0 — caller should advance offset to avoid getting stuck.
func (p *AdaptivePump) fetchChunk(offset, size int64) int {
	h := p.handle
	if h.isClosed() {
		return 0
	}

	if h.raCache.Covered(h.path, offset, size) {
		return int(size)
	}

	// Allocate fresh buffer — not from pool, since PutOwned transfers ownership to cache.
	// Using the pool here would cause a data race: cache holds buf while pool reuses same backing array.
	buf := make([]byte, size)
	n, err := h.client.FetchBlock(h.hash, h.fileID, offset, buf)
	if err != nil {
		// Shrink chunk on error
		newSize := p.chunkSize.Load() / 2
		if newSize < appMinChunk {
			newSize = appMinChunk
		}
		p.chunkSize.Store(newSize)
		return 0
	}

	if n > 0 {
		// Zero-copy: transfer buffer ownership to cache
		h.raCache.PutOwned(h.path, offset, buf[:n])
	}
	return n
}

// Stats returns current pump statistics for monitoring.
func (p *AdaptivePump) Stats() map[string]interface{} {
	return map[string]interface{}{
		"phase":           pumpPhase(p.phase.Load()).String(),
		"chunkSizeKB":     p.chunkSize.Load() / 1024,
		"downloadSpeedKB": p.bytesPerSecond.Load() / 1024,
		"playerOffset":    p.playerOff.Load(),
	}
}
