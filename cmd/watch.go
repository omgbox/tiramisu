package cmd

import (
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchDirectory monitors a directory for new .torrent files and processes them.
func WatchDirectory(dataDir string, onTorrent func(string)) {
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

	for event := range watcher.Events {
		if event.Op&fsnotify.Create != 0 && strings.HasSuffix(event.Name, ".torrent") {
			// Debounce: wait 500ms for file to finish writing
			name := event.Name
			if t, ok := debounce[name]; ok {
				t.Stop()
			}
			debounce[name] = time.AfterFunc(500*time.Millisecond, func() {
				log.Printf("[Watch] New torrent detected: %s", filepath.Base(name))
				onTorrent(name)
				delete(debounce, name)
			})
		}
	}
}
