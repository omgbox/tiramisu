package fusefs

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"bittorrentfs/internal/streaming"
	"bittorrentfs/internal/stub"

	"github.com/winfsp/cgofuse/fuse"
)

// BitTorrentFS implements fuse.FileSystemInterface for torrent streaming.
type BitTorrentFS struct {
	fuse.FileSystemBase
	dataDir   string
	raCache   *streaming.ReadAheadCache
	client    streaming.NativeClient
	handles   sync.Map // path -> *streaming.MkvHandle
	metaCache sync.Map // path -> *stub.StubMeta
	host      *fuse.FileSystemHost
	pumpSem   *streaming.MasterSemaphore // limits concurrent read-ahead pumps

	// Readdir cache — prevents Explorer from hammering the filesystem
	dirCacheMu sync.RWMutex
	dirCache   map[string]dirCacheEntry

	// Handle cleanup
	handleCleanupStop chan struct{}
}

type dirCacheEntry struct {
	entries  []os.DirEntry
	created  time.Time
}

// NewBitTorrentFS creates a new FUSE filesystem.
func NewBitTorrentFS(dataDir string, client streaming.NativeClient, raCache *streaming.ReadAheadCache) *BitTorrentFS {
	// Scale pump semaphore: 4 base + up to 4 more for concurrent streams
	pumpSem := streaming.NewMasterSemaphore(8)

	fs := &BitTorrentFS{
		dataDir:           dataDir,
		raCache:           raCache,
		client:            client,
		pumpSem:           pumpSem,
		dirCache:          make(map[string]dirCacheEntry),
		handleCleanupStop: make(chan struct{}),
	}
	go fs.handleCleanupLoop()
	return fs
}

// handleCleanupLoop periodically closes idle handles to prevent goroutine leaks.
func (fs *BitTorrentFS) handleCleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-fs.handleCleanupStop:
			return
		case <-ticker.C:
			fs.handles.Range(func(key, val any) bool {
				h := val.(*streaming.MkvHandle)
			// Close handles idle for 5min that aren't actively being read
			if !h.IsActive(5 * time.Minute) {
					path := key.(string)
					log.Printf("[FUSE] Closing idle handle: %s", path)
					h.Close()
					fs.handles.Delete(key)
				}
				return true
			})
		}
	}
}

// Mount starts the FUSE mount at the given point (blocks).
func (fs *BitTorrentFS) Mount(mountPoint string) {
	fs.host = fuse.NewFileSystemHost(fs)
	log.Printf("[FUSE] Mounting at %s", mountPoint)
	fs.host.Mount(mountPoint, nil)
}

// Unmount unmounts the FUSE filesystem.
func (fs *BitTorrentFS) Unmount() {
	close(fs.handleCleanupStop)
	// Close all handles
	fs.handles.Range(func(key, val any) bool {
		val.(*streaming.MkvHandle).Close()
		fs.handles.Delete(key)
		return true
	})
	if fs.host != nil {
		fs.host.Unmount()
	}
}

func (fs *BitTorrentFS) getOrReadMeta(path string) (*stub.StubMeta, error) {
	if val, ok := fs.metaCache.Load(path); ok {
		return val.(*stub.StubMeta), nil
	}
	meta, err := stub.ParseStub(path)
	if err != nil {
		return nil, err
	}
	fs.metaCache.Store(path, meta)
	return meta, nil
}

func (fs *BitTorrentFS) resolvePath(path string) string {
	if path == "/" || path == "" {
		return fs.dataDir
	}
	cleaned := strings.TrimPrefix(path, "/")
	cleaned = filepath.Clean(cleaned)
	return filepath.Join(fs.dataDir, cleaned)
}

// isVideoStub checks if a filename looks like a video stub (.mkv, .mp4, .avi, etc.)
func isVideoStub(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm":
		return true
	}
	return false
}

// Getattr returns file/directory attributes.
func (fs *BitTorrentFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	fullPath := fs.resolvePath(path)

	if path == "/" || path == "" {
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 1
		return 0
	}

	if isVideoStub(path) {
		meta, err := fs.getOrReadMeta(fullPath)
		if err == nil {
			stat.Size = meta.Size
			stat.Mode = fuse.S_IFREG | 0644
			stat.Nlink = 1
			stat.Blocks = (meta.Size + 511) / 512
			return 0
		}
	}

	fi, err := os.Lstat(fullPath)
	if err != nil {
		return -fuse.ENOENT
	}

	if fi.IsDir() {
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 1
	} else {
		stat.Size = fi.Size()
		stat.Mode = fuse.S_IFREG | 0644
		stat.Nlink = 1
		stat.Blocks = (fi.Size() + 511) / 512
	}
	return 0
}

// Readdir lists directory contents using the cgofuse fill callback.
func (fs *BitTorrentFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, off int64) bool, off int64, fh uint64) int {
	fullPath := fs.resolvePath(path)

	// Check cache (500ms TTL — prevents Explorer from hammering)
	entries := fs.getCachedDir(fullPath)
	if entries == nil {
		raw, err := os.ReadDir(fullPath)
		if err != nil {
			return -fuse.ENOENT
		}
		// Filter: skip zero-byte stubs mid-write, skip non-video, non-torrent, non-dir entries
		entries = make([]os.DirEntry, 0, len(raw))
		for _, e := range raw {
			name := e.Name()
			if e.IsDir() || strings.HasSuffix(name, ".torrent") {
				entries = append(entries, e)
				continue
			}
			if isVideoStub(name) {
				// Skip stubs that are0 bytes (being written)
				if !e.IsDir() {
					if info, err := e.Info(); err == nil && info.Size() == 0 {
						continue
					}
				}
				entries = append(entries, e)
				continue
			}
		}
		fs.setCachedDir(fullPath, entries)
	}

	idx := int64(0)
	for _, e := range entries {
		name := e.Name()
		if idx >= off {
			stat := &fuse.Stat_t{}
			if e.IsDir() {
				stat.Mode = fuse.S_IFDIR | 0755
			} else {
				stat.Mode = fuse.S_IFREG | 0644
			}
			if !fill(name, stat, idx) {
				break
			}
		}
		idx++
	}
	return 0
}

func (fs *BitTorrentFS) getCachedDir(path string) []os.DirEntry {
	fs.dirCacheMu.RLock()
	defer fs.dirCacheMu.RUnlock()
	if entry, ok := fs.dirCache[path]; ok {
		if time.Since(entry.created) < 500*time.Millisecond {
			return entry.entries
		}
	}
	return nil
}

func (fs *BitTorrentFS) setCachedDir(path string, entries []os.DirEntry) {
	fs.dirCacheMu.Lock()
	defer fs.dirCacheMu.Unlock()
	fs.dirCache[path] = dirCacheEntry{entries: entries, created: time.Now()}
}

// InvalidateDirCache clears the cache for a specific path (call after adding files).
func (fs *BitTorrentFS) InvalidateDirCache(path string) {
	fs.dirCacheMu.Lock()
	defer fs.dirCacheMu.Unlock()
	delete(fs.dirCache, path)
}

// Open opens a file for reading.
func (fs *BitTorrentFS) Open(path string, flags int) (int, uint64) {
	if path == "/" || path == "" {
		return 0, 0
	}

	fullPath := fs.resolvePath(path)

	if _, err := fs.getOrReadMeta(fullPath); err != nil {
		return -fuse.ENOENT, 0
	}

	if _, ok := fs.handles.Load(path); ok {
		return 0, 1
	}

	meta, _ := fs.getOrReadMeta(fullPath)
	hash := stub.ExtractHash(meta.Magnet)

	h := streaming.NewHandle(streaming.HandleConfig{
		Path:     fullPath,
		URL:      meta.URL,
		Magnet:   meta.Magnet,
		Size:     meta.Size,
		Hash:     hash,
		FilePath: meta.FilePath,
		Client:   fs.client,
		Cache:    fs.raCache,
		PumpSem:  fs.pumpSem,
	})

	fs.handles.Store(path, h)
	return 0, 1
}

// Read reads data from a file.
func (fs *BitTorrentFS) Read(path string, buf []byte, off int64, fh uint64) int {
	val, ok := fs.handles.Load(path)
	if !ok {
		return -fuse.EBADF
	}

	h := val.(*streaming.MkvHandle)
	n, err := h.Read(buf, off)
	if err != nil {
		return -fuse.EIO
	}
	return n
}

// Release closes a file handle.
func (fs *BitTorrentFS) Release(path string, fh uint64) int {
	// Don't destroy the handle — VLC probes by opening/closing repeatedly.
	// Keep the handle alive so the next Open reuses it (with its reader & cached data).
	return 0
}

// Statfs returns filesystem statistics.
func (fs *BitTorrentFS) Statfs(path string, statvfs *fuse.Statfs_t) int {
	statvfs.Bsize = 4096
	statvfs.Blocks = 250 * 1024 * 1024
	statvfs.Bfree = 125 * 1024 * 1024
	statvfs.Bavail = 125 * 1024 * 1024
	return 0
}


