package cmd

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"bittorrentfs/internal/config"
	"bittorrentfs/internal/fusefs"
	"bittorrentfs/internal/gostorm/native"
	"bittorrentfs/internal/gostorm/settings"
	"bittorrentfs/internal/gostorm/torr"
	"bittorrentfs/internal/streaming"
	"bittorrentfs/internal/stub"
	"bittorrentfs/internal/warmup"
	"bittorrentfs/internal/winfsp"
)

var (
	mountPoint string
	dataDir    string
	configPath string
	verbose    bool

	// torrentHashes maps .torrent file path → info hash hex, so deletion can
	// look up the hash after the file is gone.
	torrentHashes sync.Map
)

// Execute is the main entry point for the CLI.
func Execute() {
	// Parse flags
	flag.StringVar(&mountPoint, "mount", "", "FUSE mount point (drive letter, e.g. Z:)")
	flag.StringVar(&dataDir, "data-dir", "", "Directory for .mkv stubs")
	flag.StringVar(&configPath, "config", "", "Config file path")
	flag.BoolVar(&verbose, "verbose", false, "Verbose logging")
	flag.Parse()

	// Load config
	cfg := config.LoadConfig(configPath)

	// Override config from flags
	if mountPoint != "" {
		cfg.FuseMountPoint = mountPoint
	}
	if dataDir != "" {
		cfg.DataDir = dataDir
	}

	// Set up logging
	if !verbose {
		log.SetFlags(log.LstdFlags)
	}

	cfg.LogConfig()

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Parse torrent/magnet args
	args := flag.Args()
	for _, arg := range args {
		if strings.HasPrefix(arg, "magnet:") {
			if err := addMagnet(cfg.DataDir, arg); err != nil {
				log.Printf("Failed to add magnet: %v", err)
			}
		} else if strings.HasSuffix(arg, ".torrent") {
			if _, err := addTorrent(cfg.DataDir, arg); err != nil {
				log.Printf("Failed to add torrent: %v", err)
			}
		} else {
			log.Printf("Unknown argument: %s (expected .torrent file or magnet link)", arg)
		}
	}

	// Initialize streaming core
	raCache := streaming.NewReadAheadCache(cfg.ReadAheadBudget, cfg.ReadAheadBase)

	// Initialize warmup
	warmupDir := filepath.Join(cfg.RootPath, "warmup")
	warmup.InitDiskWarmup(warmupDir, cfg.DiskWarmupQuotaGB)

	// Initialize GoStorm settings (no HTTP server)
	settingsPath := filepath.Join(cfg.RootPath, "gostorm")
	if err := os.MkdirAll(settingsPath, 0755); err != nil {
		log.Fatalf("Failed to create gostorm dir: %v", err)
	}
	settings.Path = settingsPath
	settings.Args = &settings.ExecArgs{
		Port: "0",
		IP:   "127.0.0.1",
		Path: settingsPath,
	}
	log.Printf("[GoStorm] Initializing at %s", settingsPath)
	settings.InitSets(false, false)

	// Initialize torrent engine directly (no web server)
	bts := torr.NewBTS()
	if err := bts.Connect(); err != nil {
		log.Fatalf("Failed to connect torrent engine: %v", err)
	}
	log.Printf("[GoStorm] Torrent engine connected")

	// Create native client bridge
	nativeClient := native.NewNativeClient()
	client := streaming.NewGostormAdapter(nativeClient)

	// Create FUSE filesystem
	fs := fusefs.NewBitTorrentFS(cfg.DataDir, client, raCache)

	// Scan for existing .torrent files in data dir (watcher only catches new ones)
	entries, err := os.ReadDir(cfg.DataDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".torrent") {
				fullPath := filepath.Join(cfg.DataDir, e.Name())
				// Track for deletion detection
				if info, infoErr := os.Stat(fullPath); infoErr == nil {
					knownTorrents.Store(fullPath, info.ModTime())
					log.Printf("[Start] Tracking torrent for cleanup: %s", e.Name())
				}
				log.Printf("[Start] Found existing torrent: %s", e.Name())
				stubs, err := addTorrent(cfg.DataDir, fullPath)
				if err != nil {
					log.Printf("[Start] Failed to process torrent: %v", err)
					continue
				}
			// Pre-wake: start torrent engine early so peers are discovered before VLC opens the file
			for _, stubPath := range stubs {
				meta, parseErr := stub.ParseStub(stubPath)
				if parseErr != nil {
					continue
				}
				hash := stub.ExtractHash(meta.Magnet)
				log.Printf("[Start] Pre-waking torrent %s (hash=%s)", e.Name(), hash[:8])
					if wakeErr := client.Wake(meta.Magnet, 0); wakeErr != nil {
						log.Printf("[Start] Pre-wake failed for %s: %v", e.Name(), wakeErr)
					}
				}
			}
		}
	}

	// Start watched directory for new .torrent files
	go WatchDirectory(cfg.DataDir, func(path string) {
		log.Printf("[Watch] Processing new torrent: %s", filepath.Base(path))
		stubs, err := addTorrent(cfg.DataDir, path)
		if err != nil {
			log.Printf("[Watch] Failed to process torrent: %v", err)
			return
		}
		// Track for deletion detection (addTorrent stores hash, but also track path here)
		if info, infoErr := os.Stat(path); infoErr == nil {
			knownTorrents.Store(path, info.ModTime())
		}
		// Pre-wake for new torrents too
		for _, stubPath := range stubs {
			meta, parseErr := stub.ParseStub(stubPath)
			if parseErr != nil {
				continue
			}
			hash := stub.ExtractHash(meta.Magnet)
			log.Printf("[Watch] Pre-waking torrent %s (hash=%s)", filepath.Base(path), hash[:8])
			if wakeErr := client.Wake(meta.Magnet, 0); wakeErr != nil {
				log.Printf("[Watch] Pre-wake failed for %s: %v", filepath.Base(path), wakeErr)
			}
		}
	}, func(path string) {
		removeTorrent(cfg.DataDir, path, client, fs, raCache)
	})

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		fs.Unmount()
	}()

	// Auto-install WinFsp if not present
	if err := winfsp.Ensure(); err != nil {
		log.Fatalf("[BitTorrentFS] WinFsp installation failed: %v", err)
	}

	// Mount FUSE and block
	log.Printf("[BitTorrentFS] Ready — mount at %s", cfg.FuseMountPoint)
	fs.Mount(cfg.FuseMountPoint)
}

func addTorrent(dataDir, torrentPath string) ([]string, error) {
	absPath, err := filepath.Abs(torrentPath)
	if err != nil {
		return nil, fmt.Errorf("resolve path: %w", err)
	}

	stubs, err := stub.CreateStubsFromTorrent(dataDir, absPath)
	if err != nil {
		return nil, fmt.Errorf("create stubs: %w", err)
	}

	// Store hash for later cleanup on deletion
	if len(stubs) > 0 {
		if meta, err := stub.ParseStub(stubs[0]); err == nil {
			hash := stub.ExtractHash(meta.Magnet)
			torrentHashes.Store(absPath, hash)
		}
	}

	for _, s := range stubs {
		log.Printf("Created stub: %s", s)
	}
	return stubs, nil
}

func addMagnet(dataDir, magnetURI string) error {
	stubPath, err := stub.CreateStubFromMagnet(dataDir, magnetURI)
	if err != nil {
		return fmt.Errorf("create stub: %v", err)
	}

	log.Printf("Created stub: %s", stubPath)
	return nil
}

// removeTorrent cleans up a torrent whose .torrent file was deleted from the
// watched directory. Removes stubs, FUSE handle, DB entry, and cache.
func removeTorrent(dataDir, torrentPath string, client *streaming.GostormAdapter, fs *fusefs.BitTorrentFS, raCache *streaming.ReadAheadCache) {
	absPath, err := filepath.Abs(torrentPath)
	if err != nil {
		log.Printf("[Watch] Failed to resolve path for removal: %v", err)
		return
	}

	// Look up hash from the map (populated at add time)
	val, ok := torrentHashes.Load(absPath)
	if !ok {
		log.Printf("[Watch] No hash recorded for %s, scanning stubs", filepath.Base(absPath))
		// Fallback: scan all stubs to find matching magnet hash
		// (handles case where process restarted and map is empty)
		entries, readErr := os.ReadDir(dataDir)
		if readErr != nil {
			return
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			stubDir := filepath.Join(dataDir, e.Name())
			stubFiles, _ := os.ReadDir(stubDir)
			for _, sf := range stubFiles {
				if sf.IsDir() || !strings.HasSuffix(sf.Name(), ".mkv") {
					continue
				}
				meta, parseErr := stub.ParseStub(filepath.Join(stubDir, sf.Name()))
				if parseErr != nil {
					continue
				}
				h := stub.ExtractHash(meta.Magnet)
				if h != "" {
					torrentHashes.Store(absPath, h)
					val = h
					ok = true
					break
				}
			}
			if ok {
				break
			}
		}
		if !ok {
			log.Printf("[Watch] Could not determine hash for %s", filepath.Base(absPath))
			return
		}
	}

	hash := val.(string)
	log.Printf("[Watch] Removing torrent %s (hash=%s)", filepath.Base(absPath), hash[:8])

	// 1. Remove stubs from disk
	removed := stub.RemoveStubs(dataDir, hash)
	log.Printf("[Watch] Removed %d stub files", removed)

	// 2. Close FUSE handle for this torrent
	fs.CloseHandleByHash(hash)

	// 3. Remove torrent from GoStorm engine + DB
	if err := client.RemoveTorrent(hash); err != nil {
		log.Printf("[Watch] RemoveTorrent failed: %v", err)
	}

	// 4. Clean up hash map entry
	torrentHashes.Delete(absPath)

	log.Printf("[Watch] Cleanup complete for %s", filepath.Base(absPath))
}
