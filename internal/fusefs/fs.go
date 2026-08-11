package fusefs

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"tiramisu/internal/streaming"
	"tiramisu/internal/stub"
	"tiramisu/internal/warmup"

	"github.com/winfsp/cgofuse"
)

// TiramisuFS implements cgofuse.FileSystemInterface for torrent streaming.
type TiramisuFS struct {
	cgofuse.FileSystemBase
	dataDir     string
	raCache     *streaming.ReadAheadCache
	client      streaming.NativeClient
	handles     sync.Map // path -> *streaming.MkvHandle
	metaCache   sync.Map // path -> *stub.StubMeta
	host        *cgofuse.FileSystemHost
}

// NewTiramisuFS creates a new FUSE filesystem.
func NewTiramisuFS(dataDir string, client streaming.NativeClient, raCache *streaming.ReadAheadCache) *TiramisuFS {
	return &TiramisuFS{
		dataDir: dataDir,
		raCache: raCache,
		client:  client,
	}
}

// Mount starts the FUSE mount at the given point.
func (fs *TiramisuFS) Mount(mountPoint string) {
	fs.host = cgofuse.NewFileSystemHost(fs)
	log.Printf("[FUSE] Mounting at %s", mountPoint)
	fs.host.Mount(mountPoint, nil)
}

// Unmount unmounts the FUSE filesystem.
func (fs *TiramisuFS) Unmount() {
	if fs.host != nil {
		fs.host.Unmount()
	}
}

// Wait blocks until the FUSE filesystem is unmounted.
func (fs *TiramisuFS) Wait() {
	if fs.host != nil {
		fs.host.Wait()
	}
}

func (fs *TiramisuFS) getOrReadMeta(path string) (*stub.StubMeta, error) {
	// Check cache
	if val, ok := fs.metaCache.Load(path); ok {
		return val.(*stub.StubMeta), nil
	}

	// Read from disk
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
	return filepath.Join(fs.dataDir, filepath.Clean(path))
}

// Getattr returns file/directory attributes.
func (fs *TiramisuFS) Getattr(path string, stat *cgofuse.Stat_t, fh uint64) int {
	fullPath := fs.resolvePath(path)

	if path == "/" || path == "" {
		// Root directory
		stat.Mode = cgofuse.S_IFDIR | 0755
		stat.Nlink = 1
		stat.Blksize = 1048576
		return 0
	}

	// Try as .mkv stub
	if strings.HasSuffix(path, ".mkv") {
		meta, err := fs.getOrReadMeta(fullPath)
		if err == nil {
			stat.Size = meta.Size
			stat.Mode = cgofuse.S_IFREG | 0644
			stat.Nlink = 1
			stat.Blksize = 1048576
			stat.Blocks = (meta.Size + 511) / 512
			stat.Mtim = cgofuse.NewTimespec(time.Now())
			stat.Atim = cgofuse.NewTimespec(time.Now())
			stat.Ctim = cgofuse.NewTimespec(time.Now())
			return 0
		}
	}

	// Try as real file/directory
	fi, err := os.Lstat(fullPath)
	if err != nil {
		return -cgofuse.ENOENT
	}

	if fi.IsDir() {
		stat.Mode = cgofuse.S_IFDIR | 0755
		stat.Nlink = 1
	} else {
		stat.Size = fi.Size()
		stat.Mode = cgofuse.S_IFREG | 0644
		stat.Nlink = 1
		stat.Blocks = (fi.Size() + 511) / 512
	}

	stat.Blksize = 1048576
	stat.Mtim = cgofuse.NewTimespec(fi.ModTime())
	stat.Atim = cgofuse.NewTimespec(fi.ModTime())
	stat.Ctim = cgofuse.NewTimespec(fi.ModTime())
	return 0
}

// Readdir lists directory contents.
func (fs *TiramisuFS) Readdir(path string, fills []cgofuse.DirEntry, off int64, fh uint64) int {
	fullPath := fs.resolvePath(path)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return -cgofuse.ENOENT
	}

	idx := 0
	for _, e := range entries {
		name := e.Name()

		// For .mkv stubs, show the stub filename
		// For directories, show them
		if e.IsDir() || strings.HasSuffix(name, ".mkv") || strings.HasSuffix(name, ".torrent") {
			if idx >= int(off) {
				fills = append(fills, cgofuse.DirEntry{
					Name: name,
				})
			}
			idx++
		}
	}

	// Also show .mkv files from stubs in subdirectories
	if path == "/" || path == "" {
		subDirs, _ := os.ReadDir(fullPath)
		for _, d := range subDirs {
			if d.IsDir() {
				subPath := filepath.Join(fullPath, d.Name())
				subEntries, _ := os.ReadDir(subPath)
				for _, se := range subEntries {
					if strings.HasSuffix(se.Name(), ".mkv") {
						name := d.Name() + "/" + se.Name()
						if idx >= int(off) {
							fills = append(fills, cgofuse.DirEntry{
								Name: name,
							})
						}
						idx++
					}
				}
			}
		}
	}

	return 0
}

// Open opens a file for reading.
func (fs *TiramisuFS) Open(path string, flags uint64, fh uint64) (int, uint64) {
	fullPath := fs.resolvePath(path)

	// Root always opens
	if path == "/" || path == "" {
		return 0, 0
	}

	meta, err := fs.getOrReadMeta(fullPath)
	if err != nil {
		return -cgofuse.ENOENT, 0
	}

	// Check if handle already exists
	if val, ok := fs.handles.Load(path); ok {
		h := val.(*streaming.MkvHandle)
		return 0, uint64(uintptr(h))
	}

	// Extract hash from magnet
	hash := stub.ExtractHash(meta.Magnet)
	fileIdx := 0

	// Create handle
	h := streaming.NewHandle(streaming.HandleConfig{
		Path:   fullPath,
		URL:    meta.URL,
		Magnet: meta.Magnet,
		Size:   meta.Size,
		Hash:   hash,
		FileID: fileIdx,
		Client: fs.client,
		Cache:  fs.raCache,
	})

	fs.handles.Store(path, h)
	return 0, 0
}

// Read reads data from a file.
func (fs *TiramisuFS) Read(path string, buf []byte, off int64, fh uint64) int {
	val, ok := fs.handles.Load(path)
	if !ok {
		return -cgofuse.EBADF
	}

	h := val.(*streaming.MkvHandle)
	n, err := h.Read(buf, off)
	if err != nil {
		log.Printf("[FUSE] Read error for %s: %v", path, err)
		return -cgofuse.EIO
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
func (fs *TiramisuFS) Statfs(path string, statvfs *cgofuse.Statfs_t) int {
	statvfs.Bsize = 4096
	statvfs.Blocks = 250 * 1024 * 1024 // 1TB
	statvfs.Bfree = 125 * 1024 * 1024
	statvfs.Bavail = 125 * 1024 * 1024
	statvfs.NameLen = 255
	return 0
}

// Init is called when the FUSE filesystem is mounted.
func (fs *TiramisuFS) Init() {
	log.Printf("[FUSE] Filesystem initialized, dataDir=%s", fs.dataDir)
}
