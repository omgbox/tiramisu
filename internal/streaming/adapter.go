package streaming

import (
	"tiramisu/internal/gostorm/native"
)

// GostormAdapter wraps the gostorm NativeClient to implement the streaming.NativeClient interface.
type GostormAdapter struct {
	inner *native.NativeClient
}

// NewGostormAdapter creates an adapter from gostorm's NativeClient.
func NewGostormAdapter(client *native.NativeClient) *GostormAdapter {
	return &GostormAdapter{inner: client}
}

// Wake triggers torrent metadata fetch and peer discovery.
func (a *GostormAdapter) Wake(magnet string, fileIdx int) error {
	return a.inner.Wake(magnet, fileIdx)
}

// NewStreamReader creates a streaming reader for a torrent file.
func (a *GostormAdapter) NewStreamReader(hash string, fileID int, totalSize int64) NativeReader {
	inner := a.inner.NewStreamReader(hash, fileID, totalSize)
	return &GostormReaderAdapter{inner: inner}
}

// FetchBlock performs a synchronous single-chunk fetch from the torrent.
func (a *GostormAdapter) FetchBlock(hash string, fileID int, offset int64, buf []byte) (int, error) {
	return a.inner.FetchBlock(hash, fileID, offset, buf)
}

// RemoveTorrent removes a torrent from the engine.
func (a *GostormAdapter) RemoveTorrent(hash string) error {
	return a.inner.RemoveTorrent(hash)
}

// GostormReaderAdapter wraps gostorm's NativeReader to implement streaming.NativeReader.
type GostormReaderAdapter struct {
	inner *native.NativeReader
}

// ReadAt reads bytes from the torrent at the given offset.
func (r *GostormReaderAdapter) ReadAt(p []byte, off int64) (n int, err error) {
	return r.inner.ReadAt(p, off)
}

// Interrupt unblocks a pending ReadAt call (used for seek handling).
func (r *GostormReaderAdapter) Interrupt() {
	r.inner.Interrupt()
}

// Close closes the reader and releases resources.
func (r *GostormReaderAdapter) Close() error {
	return r.inner.Close()
}
