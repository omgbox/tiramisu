package cmd

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

var knownTorrents sync.Map  // path → lastModTime
var pendingRemovals sync.Map // path → *time.Timer

// openForRead opens a file with FILE_SHARE_DELETE on Windows so that the
// file can be deleted by the user while we hold it open. Without this flag,
// Windows marks the file "delete pending" and it stays on disk until we close.
func openForRead(path string) (*os.File, error) {
	if runtime.GOOS != "windows" {
		return os.Open(path)
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return os.Open(path)
	}
	h, err := syscall.CreateFile(
		pathp,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(h), path), nil
}

func WatchDirectory(dataDir string, onTorrent func(string), onTorrentRemoved func(string)) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("[Watch] Failed to create watcher: %v", err)
		return
	}
	defer watcher.Close()

	if err := watcher.Add(dataDir); err != nil {
		log.Printf("[Watch] Failed to watch %s: %v", dataDir, err)
		return
	}

	log.Printf("[Watch] Monitoring %s for .torrent files", dataDir)

	debounce := make(map[string]*time.Timer)

	// Periodic scanner: catches deletions that fsnotify misses on Windows.
	// Opens each file with FILE_SHARE_DELETE to detect gone or truncated files.
	go func() {
		defer func() { recover() }()
		for {
			time.Sleep(2 * time.Second)
			knownTorrents.Range(func(key, val any) bool {
				path := key.(string)
				f, err := openForRead(path)
				if err != nil {
					knownTorrents.Delete(path)
					if onTorrentRemoved != nil {
						log.Printf("[Watch] Detected deleted torrent: %s", filepath.Base(path))
						onTorrentRemoved(path)
					}
					return true
				}
				buf := make([]byte, 1)
				n, readErr := f.Read(buf)
				f.Close()
				if n == 0 || readErr != nil {
					knownTorrents.Delete(path)
					if onTorrentRemoved != nil {
						log.Printf("[Watch] Detected truncated torrent: %s", filepath.Base(path))
						onTorrentRemoved(path)
					}
				}
				return true
			})
		}
	}()

	for event := range watcher.Events {
		if !strings.HasSuffix(event.Name, ".torrent") {
			continue
		}

		switch {
		case event.Op&fsnotify.Create != 0:
			name := event.Name
			// Cancel any pending removal for this file (re-created during write)
			if t, ok := pendingRemovals.LoadAndDelete(name); ok {
				t.(*time.Timer).Stop()
			}
			if t, ok := debounce[name]; ok {
				t.Stop()
			}
			debounce[name] = time.AfterFunc(500*time.Millisecond, func() {
				log.Printf("[Watch] New torrent detected: %s", filepath.Base(name))
				onTorrent(name)
				delete(debounce, name)
			})
			if info, err := os.Stat(name); err == nil {
				knownTorrents.Store(name, info.ModTime())
			}

		case event.Op&fsnotify.Remove != 0 || event.Op&fsnotify.Rename != 0:
			name := event.Name
			if t, ok := debounce[name]; ok {
				t.Stop()
				delete(debounce, name)
			}
			// Schedule delayed verification: Windows fires Remove→Create during
			// file writes (temp→rename). If the file re-appears within 3s, the
			// pending removal is cancelled by the Create handler above.
			if old, ok := pendingRemovals.LoadAndDelete(name); ok {
				old.(*time.Timer).Stop()
			}
			timer := time.AfterFunc(3*time.Second, func() {
				pendingRemovals.Delete(name)
				knownTorrents.Delete(name)
				if onTorrentRemoved != nil {
					log.Printf("[Watch] Confirmed deletion: %s", filepath.Base(name))
					onTorrentRemoved(name)
				}
			})
			pendingRemovals.Store(name, timer)

		case event.Op&fsnotify.Write != 0:
			if info, err := os.Stat(event.Name); err == nil {
				knownTorrents.Store(event.Name, info.ModTime())
			}
		}
	}
}
