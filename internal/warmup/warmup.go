package warmup

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FileSize is the per-file head cache cap. Set at init from config, default 64 MB.
var FileSize int64 = 64 * 1024 * 1024

const (
	TailWarmupSize int64 = 16 * 1024 * 1024        // 16 MB tail (Cues/seek index)
	warmupSuffix         = ".warmup"
	tailSuffix           = ".warmup-tail"
	warmupWriteBuf       = 16 * 1024 * 1024 // 16 MB — matches pump chunk size
	handleIdleMax        = 30 * time.Second // close idle file handles after 30s
)

var diskQuotaGB int64

// DiskWarmup is the global instance, nil when disabled.
var DiskWarmup *DiskWarmupCache

var warmupDurationBuckets [8]atomic.Int64 // <2s,<5s,<10s,<15s,<30s,<60s,<120s,>=120s

func recordWarmupDuration(d time.Duration) {
	s := d.Seconds()
	idx := 7
	switch {
	case s < 2:
		idx = 0
	case s < 5:
		idx = 1
	case s < 10:
		idx = 2
	case s < 15:
		idx = 3
	case s < 30:
		idx = 4
	case s < 60:
		idx = 5
	case s < 120:
		idx = 6
	}
	warmupDurationBuckets[idx].Add(1)
}

// WarmupDurationBucketCounts returns a snapshot of the bucket counts.
func WarmupDurationBucketCounts() [8]int64 {
	var out [8]int64
	for i := range warmupDurationBuckets {
		out[i] = warmupDurationBuckets[i].Load()
	}
	return out
}

var warmupWritePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, warmupWriteBuf)
		return &buf
	},
}

type warmupWrite struct {
	hash   string
	fileID int
	buf    *[]byte
	len    int
	off    int64
}

type cachedHandle struct {
	f            *os.File
	lastUsedNano atomic.Int64
	closed       atomic.Bool
}

type sizeEntry struct {
	size      int64
	updatedAt time.Time
}

type tailRange struct {
	mu            sync.Mutex
	highWatermark int64
}

// DiskWarmupCache persists the first N MB of each streamed file to SSD.
type DiskWarmupCache struct {
	dir          string
	mu           sync.Mutex
	totalSize    int64
	missing      sync.Map
	handles      sync.Map
	sizeCache    sync.Map
	tailCoverage sync.Map
	warmupStarts sync.Map
	writeCh      chan warmupWrite
}

var logf = log.New(os.Stdout, "[DiskWarmup] ", log.LstdFlags)

// InitDiskWarmup creates the global warmup cache.
func InitDiskWarmup(dir string, quotaGB int64) {
	diskQuotaGB = quotaGB
	FileSize = 64 * 1024 * 1024

	if dir == "" || dir == "/" {
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		logf.Printf("Failed to create warmup dir: %v", err)
		return
	}

	DiskWarmup = &DiskWarmupCache{
		dir:     dir,
		writeCh: make(chan warmupWrite, 32),
	}

	if entries, err := os.ReadDir(dir); err == nil {
		var initialTotal int64
		for _, e := range entries {
			name := e.Name()
			if strings.HasSuffix(name, warmupSuffix) || strings.HasSuffix(name, tailSuffix) {
				if info, err := e.Info(); err == nil {
					initialTotal += info.Size()
				}
			}
		}
		atomic.StoreInt64(&DiskWarmup.totalSize, initialTotal)
		logf.Printf("Initial size: %.1fGB", float64(initialTotal)/(1<<30))
	}

	go DiskWarmup.writeWorker()
	go DiskWarmup.handleReaper()

	logf.Printf("Active — dir=%s quota=%dGB warmup=%dMB", dir, quotaGB, FileSize/1024/1024)
}

func (d *DiskWarmupCache) handleReaper() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		d.handles.Range(func(key, val interface{}) bool {
			ch := val.(*cachedHandle)
			if now.UnixNano()-ch.lastUsedNano.Load() > handleIdleMax.Nanoseconds() {
				if actual, loaded := d.handles.LoadAndDelete(key); loaded {
					ac := actual.(*cachedHandle)
					if now.UnixNano()-ac.lastUsedNano.Load() < handleIdleMax.Nanoseconds() {
						d.handles.Store(key, ac)
						return true
					}
					ac.closed.Store(true)
					ac.f.Close()
					d.warmupStarts.Delete(key)
				}
			}
			return true
		})
	}
}

func (d *DiskWarmupCache) getHandle(path string) (*cachedHandle, error) {
	if val, ok := d.handles.Load(path); ok {
		ch := val.(*cachedHandle)
		ch.lastUsedNano.Store(time.Now().UnixNano())
		return ch, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	ch := &cachedHandle{f: f}
	ch.lastUsedNano.Store(time.Now().UnixNano())
	if actual, loaded := d.handles.LoadOrStore(path, ch); loaded {
		f.Close()
		existing := actual.(*cachedHandle)
		existing.lastUsedNano.Store(time.Now().UnixNano())
		return existing, nil
	}
	return ch, nil
}

func (d *DiskWarmupCache) closeHandle(path string) {
	if val, ok := d.handles.LoadAndDelete(path); ok {
		ch := val.(*cachedHandle)
		ch.closed.Store(true)
		ch.f.Close()
	}
	d.warmupStarts.Delete(path)
}

func (d *DiskWarmupCache) writeWorker() {
	for w := range d.writeCh {
		d.processWrite(w.hash, w.fileID, (*w.buf)[:w.len], w.off)
		warmupWritePool.Put(w.buf)
	}
}

// WriteChunk writes head warmup data asynchronously.
func (d *DiskWarmupCache) WriteChunk(hash string, fileID int, data []byte, off int64) {
	if off > FileSize || d.writeCh == nil {
		return
	}

	bufPtr := warmupWritePool.Get().(*[]byte)
	if len(*bufPtr) < len(data) {
		warmupWritePool.Put(bufPtr)
		buf := make([]byte, len(data))
		copy(buf, data)
		bufPtr = &buf
	} else {
		copy(*bufPtr, data)
	}

	select {
	case d.writeCh <- warmupWrite{hash, fileID, bufPtr, len(data), off}:
	default:
		warmupWritePool.Put(bufPtr)
	}
}

func (d *DiskWarmupCache) processWrite(hash string, fileID int, data []byte, off int64) {
	if off > FileSize {
		return
	}
	if off < FileSize && off+int64(len(data)) > FileSize {
		data = data[:FileSize-off]
	}

	path := d.filePath(hash, fileID)

	if val, ok := d.sizeCache.Load(path); ok {
		if entry := val.(sizeEntry); entry.size > FileSize {
			return
		}
	} else if fi, err := os.Stat(path); err == nil && fi.Size() > FileSize {
		d.sizeCache.Store(path, sizeEntry{size: fi.Size(), updatedAt: time.Now()})
		return
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(d.dir, 0755)

		d.mu.Lock()
		d.enforceQuotaLocked(FileSize)
		d.mu.Unlock()
		d.missing.Delete(path)

		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			logf.Printf("Error creating file: %v", err)
			return
		}
		newCh := &cachedHandle{f: f}
		newCh.lastUsedNano.Store(time.Now().UnixNano())
		d.handles.Store(path, newCh)
		d.warmupStarts.Store(path, time.Now())
		logf.Printf("STARTING %s at offset %d", filepath.Base(path), off)
	}

	ch, err := d.getHandle(path)
	if err != nil {
		return
	}
	if ch.closed.Load() {
		return
	}

	var prevSize int64
	if val, ok := d.sizeCache.Load(path); ok {
		prevSize = val.(sizeEntry).size
	} else if fi, err := ch.f.Stat(); err == nil {
		prevSize = fi.Size()
	}

	n, err := ch.f.WriteAt(data, off)
	if err != nil {
		logf.Printf("WriteAt error for %s: %v", filepath.Base(path), err)
		return
	}

	currentSize := off + int64(n)
	if currentSize > prevSize {
		atomic.AddInt64(&d.totalSize, currentSize-prevSize)
	}

	d.sizeCache.Store(path, sizeEntry{size: currentSize, updatedAt: time.Now()})

	if off+int64(n) >= FileSize {
		if startVal, ok := d.warmupStarts.LoadAndDelete(path); ok {
			recordWarmupDuration(time.Since(startVal.(time.Time)))
		}
		logf.Printf("COMPLETED %s", filepath.Base(path))
	}
}

func (d *DiskWarmupCache) filePath(hash string, fileID int) string {
	return filepath.Join(d.dir, hash+"-"+strconv.Itoa(fileID)+warmupSuffix)
}

func (d *DiskWarmupCache) tailPath(hash string, fileID int) string {
	return filepath.Join(d.dir, hash+"-"+strconv.Itoa(fileID)+tailSuffix)
}

// GetAvailableRange returns how many bytes of head warmup are available.
func (d *DiskWarmupCache) GetAvailableRange(hash string, fileID int) int64 {
	path := d.filePath(hash, fileID)
	if _, ok := d.missing.Load(path); ok {
		return 0
	}

	if val, ok := d.sizeCache.Load(path); ok {
		entry := val.(sizeEntry)
		if time.Since(entry.updatedAt) < 10*time.Second {
			return entry.size
		}
	}

	fi, err := os.Stat(path)
	if err != nil {
		d.missing.Store(path, time.Now())
		return 0
	}

	d.sizeCache.Store(path, sizeEntry{size: fi.Size(), updatedAt: time.Now()})
	return fi.Size()
}

// TailReady reports whether the tail warmup file is fully covered.
func (d *DiskWarmupCache) TailReady(hash string, fileID int) bool {
	if d == nil {
		return false
	}
	path := d.tailPath(hash, fileID)
	if val, ok := d.tailCoverage.Load(path); ok {
		tr := val.(*tailRange)
		tr.mu.Lock()
		done := tr.highWatermark >= TailWarmupSize
		tr.mu.Unlock()
		return done
	}
	fi, err := os.Stat(path)
	return err == nil && fi.Size() >= TailWarmupSize
}

// ReadAt reads from the head warmup file.
func (d *DiskWarmupCache) ReadAt(hash string, fileID int, buf []byte, off int64) (int, error) {
	if off > FileSize {
		return 0, nil
	}
	path := d.filePath(hash, fileID)

	ch, err := d.getHandle(path)
	if err != nil {
		return 0, nil
	}

	availSize := d.GetAvailableRange(hash, fileID)
	if off >= availSize {
		return 0, nil
	}

	if avail := availSize - off; int64(len(buf)) > avail {
		buf = buf[:avail]
	}

	n, err := ch.f.ReadAt(buf, off)
	return n, err
}

// WriteTail writes tail warmup data.
func (d *DiskWarmupCache) WriteTail(hash string, fileID int, data []byte, absoluteOffset, fileSize int64) {
	path := d.tailPath(hash, fileID)

	tailStart := fileSize - TailWarmupSize
	if tailStart < 0 {
		tailStart = 0
	}
	if absoluteOffset < tailStart {
		return
	}

	relOffset := absoluteOffset - tailStart
	if relOffset+int64(len(data)) > TailWarmupSize {
		data = data[:TailWarmupSize-relOffset]
	}
	if len(data) == 0 {
		return
	}

	if val, ok := d.tailCoverage.Load(path); ok {
		tr := val.(*tailRange)
		tr.mu.Lock()
		done := tr.highWatermark >= TailWarmupSize
		tr.mu.Unlock()
		if done {
			return
		}
	} else if fi, err := os.Stat(path); err == nil && fi.Size() >= TailWarmupSize {
		d.tailCoverage.Store(path, &tailRange{highWatermark: fi.Size()})
		return
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(d.dir, 0755)
		d.missing.Delete(path)

		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return
		}
		tailCh := &cachedHandle{f: f}
		tailCh.lastUsedNano.Store(time.Now().UnixNano())
		d.handles.Store(path, tailCh)
		logf.Printf("TAIL STARTING %s at relOffset %d", filepath.Base(path), relOffset)
	}

	ch, err := d.getHandle(path)
	if err != nil {
		return
	}
	if ch.closed.Load() {
		return
	}

	n, _ := ch.f.WriteAt(data, relOffset)
	d.sizeCache.Store(path, sizeEntry{size: relOffset + int64(n), updatedAt: time.Now()})

	endOff := relOffset + int64(n)
	if val, ok := d.tailCoverage.Load(path); ok {
		tr := val.(*tailRange)
		tr.mu.Lock()
		if endOff > tr.highWatermark {
			tr.highWatermark = endOff
		}
		tr.mu.Unlock()
	} else {
		d.tailCoverage.Store(path, &tailRange{highWatermark: endOff})
	}
}

// ReadTail reads from the tail warmup file.
func (d *DiskWarmupCache) ReadTail(hash string, fileID int, buf []byte, absoluteOffset, fileSize int64) (int, error) {
	tailStart := fileSize - TailWarmupSize
	if tailStart < 0 {
		tailStart = 0
	}
	if absoluteOffset < tailStart {
		return 0, nil
	}

	relOffset := absoluteOffset - tailStart
	path := d.tailPath(hash, fileID)

	readEnd := relOffset + int64(len(buf))
	if val, ok := d.tailCoverage.Load(path); ok {
		tr := val.(*tailRange)
		tr.mu.Lock()
		miss := readEnd > tr.highWatermark
		tr.mu.Unlock()
		if miss {
			fi, err := os.Stat(path)
			if err != nil || fi.Size() < readEnd {
				return 0, nil
			}
		}
	} else {
		fi, err := os.Stat(path)
		if err != nil || fi.Size() < readEnd {
			return 0, nil
		}
		d.tailCoverage.Store(path, &tailRange{highWatermark: fi.Size()})
	}

	ch, err := d.getHandle(path)
	if err != nil {
		return 0, nil
	}

	n, err := ch.f.ReadAt(buf, relOffset)
	return n, err
}

// RemoveHash removes all warmup files for a hash.
func (d *DiskWarmupCache) RemoveHash(hash string) {
	entries, _ := os.ReadDir(d.dir)
	prefix := hash + "-"
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, prefix) && (strings.HasSuffix(name, warmupSuffix) || strings.HasSuffix(name, tailSuffix)) {
			fullPath := filepath.Join(d.dir, name)

			if fi, err := e.Info(); err == nil {
				atomic.AddInt64(&d.totalSize, -fi.Size())
			}

			d.closeHandle(fullPath)
			d.sizeCache.Delete(fullPath)
			d.tailCoverage.Delete(fullPath)
			os.Remove(fullPath)
		}
	}
}

func (d *DiskWarmupCache) enforceQuotaLocked(needed int64) {
	quota := int64(32 * 1024 * 1024 * 1024) // 32GB default
	if diskQuotaGB > 0 {
		quota = diskQuotaGB * 1024 * 1024 * 1024
	}

	totalSize := atomic.LoadInt64(&d.totalSize)
	if totalSize+needed <= quota {
		return
	}

	entries, _ := os.ReadDir(d.dir)
	type wFile struct {
		path    string
		size    int64
		modTime int64
	}
	var files []wFile
	var diskTotal int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, warmupSuffix) && !strings.HasSuffix(name, tailSuffix) {
			continue
		}
		if info, err := e.Info(); err == nil {
			files = append(files, wFile{filepath.Join(d.dir, name), info.Size(), info.ModTime().Unix()})
			diskTotal += info.Size()
		}
	}

	atomic.StoreInt64(&d.totalSize, diskTotal)
	if diskTotal+needed <= quota {
		return
	}

	sort.Slice(files, func(i, j int) bool { return files[i].modTime < files[j].modTime })
	for _, fi := range files {
		if diskTotal+needed <= quota {
			break
		}
		d.closeHandle(fi.path)
		d.sizeCache.Delete(fi.path)
		d.tailCoverage.Delete(fi.path)
		os.Remove(fi.path)
		diskTotal -= fi.size
	}
	atomic.StoreInt64(&d.totalSize, diskTotal)
}
