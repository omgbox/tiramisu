package fusefs

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"tiramisu/internal/streaming"
	"tiramisu/internal/stub"

	"github.com/winfsp/cgofuse/fuse"
)

// TiramisuFS implements fuse.FileSystemInterface for torrent streaming.
type TiramisuFS struct {
	fuse.FileSystemBase
	dataDir   string
	raCache   *streaming.ReadAheadCache
	client    streaming.NativeClient
	handles   sync.Map // path -> *streaming.MkvHandle
	metaCache sync.Map // path -> *stub.StubMeta
	host      *fuse.FileSystemHost
}

// NewTiramisuFS creates a new FUSE filesystem.
func NewTiramisuFS(dataDir string, client streaming.NativeClient, raCache *streaming.ReadAheadCache) *TiramisuFS {
	return &TiramisuFS{
		dataDir: dataDir,
		raCache: raCache,
		client:  client,
	}
}

// Mount starts the FUSE mount at the given point (blocks).
func (fs *TiramisuFS) Mount(mountPoint string) {
	fs.host = fuse.NewFileSystemHost(fs)
	log.Printf("[FUSE] Mounting at %s", mountPoint)
	fs.host.Mount(mountPoint, nil)
}

// Unmount unmounts the FUSE filesystem.
func (fs *TiramisuFS) Unmount() {
	if fs.host != nil {
		fs.host.Unmount()
	}
}

func (fs *TiramisuFS) getOrReadMeta(path string) (*stub.StubMeta, error) {
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

func (fs *TiramisuFS) resolvePath(path string) string {
	if path == "/" || path == "" {
		return fs.dataDir
	}
	// Strip leading slash for Windows compatibility
	cleaned := strings.TrimPrefix(path, "/")
	cleaned = filepath.Clean(cleaned)
	return filepath.Join(fs.dataDir, cleaned)
}

// Getattr returns file/directory attributes.
func (fs *TiramisuFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	fullPath := fs.resolvePath(path)

	if path == "/" || path == "" {
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 1
		return 0
	}

	if strings.HasSuffix(path, ".mkv") {
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
func (fs *TiramisuFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, off int64) bool, off int64, fh uint64) int {
	fullPath := fs.resolvePath(path)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return -fuse.ENOENT
	}

	idx := int64(0)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasSuffix(name, ".mkv") || strings.HasSuffix(name, ".torrent") {
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
	}
	return 0
}

// Open opens a file for reading.
func (fs *TiramisuFS) Open(path string, flags int) (int, uint64) {
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
		Path:   fullPath,
		URL:    meta.URL,
		Magnet: meta.Magnet,
		Size:   meta.Size,
		Hash:   hash,
		FileID: 0,
		Client: fs.client,
		Cache:  fs.raCache,
	})

	fs.handles.Store(path, h)
	return 0, 1
}

// Read reads data from a file.
func (fs *TiramisuFS) Read(path string, buf []byte, off int64, fh uint64) int {
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
func (fs *TiramisuFS) Release(path string, fh uint64) int {
	if val, ok := fs.handles.LoadAndDelete(path); ok {
		h := val.(*streaming.MkvHandle)
		h.Release()
	}
	return 0
}

// Statfs returns filesystem statistics.
func (fs *TiramisuFS) Statfs(path string, statvfs *fuse.Statfs_t) int {
	statvfs.Bsize = 4096
	statvfs.Blocks = 250 * 1024 * 1024
	statvfs.Bfree = 125 * 1024 * 1024
	statvfs.Bavail = 125 * 1024 * 1024
	return 0
}

func init() {
	_ = atomic.LoadInt64 // ensure import
}
