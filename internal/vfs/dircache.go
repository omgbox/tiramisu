package vfs

import (
	"sync"
	"time"
)

// DirEntry represents a directory entry (replaces go-fuse/v2/fuse.DirEntry).
type DirEntry struct {
	Ino  uint64
	Name string
	Mode uint32
}

// DirCacheEntry holds cached directory entries
type DirCacheEntry struct {
	Entries   []DirEntry
	ExpiresAt time.Time
}

// DirCache provides a thread-safe cache for directory listings
type DirCache struct {
	cache map[string]DirCacheEntry
	mu    sync.RWMutex
	ttl   time.Duration
}

// NewDirCache creates a new directory cache
func NewDirCache(ttl time.Duration) *DirCache {
	return &DirCache{
		cache: make(map[string]DirCacheEntry),
		ttl:   ttl,
	}
}

// Get retrieves entries for a path if they exist and aren't expired
func (dc *DirCache) Get(path string) ([]DirEntry, bool) {
	dc.mu.RLock()
	entry, exists := dc.cache[path]
	dc.mu.RUnlock()

	if !exists {
		return nil, false
	}

	if time.Now().After(entry.ExpiresAt) {
		dc.Delete(path)
		return nil, false
	}

	return entry.Entries, true
}

// Put stores entries for a path
func (dc *DirCache) Put(path string, entries []DirEntry) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entriesCopy := make([]DirEntry, len(entries))
	copy(entriesCopy, entries)

	dc.cache[path] = DirCacheEntry{
		Entries:   entriesCopy,
		ExpiresAt: time.Now().Add(dc.ttl),
	}
}

// Delete removes an entry
func (dc *DirCache) Delete(path string) {
	dc.mu.Lock()
	delete(dc.cache, path)
	dc.mu.Unlock()
}

// CleanupExpired purges expired entries
func (dc *DirCache) CleanupExpired() {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	for path, entry := range dc.cache {
		if now.After(entry.ExpiresAt) {
			delete(dc.cache, path)
		}
	}
}
