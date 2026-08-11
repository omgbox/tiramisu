package cmd

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"tiramisu/internal/config"
	"tiramisu/internal/fusefs"
	server "tiramisu/internal/gostorm"
	"tiramisu/internal/gostorm/native"
	"tiramisu/internal/streaming"
	"tiramisu/internal/stub"
	"tiramisu/internal/warmup"
)

var (
	mountPoint string
	dataDir    string
	configPath string
	verbose    bool
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
			if err := addTorrent(cfg.DataDir, arg); err != nil {
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

	// Start GoStorm torrent engine
	log.Printf("[GoStorm] Starting torrent engine...")
	server.Start()

	// Create native client bridge
	nativeClient := native.NewNativeClient()
	client := streaming.NewGostormAdapter(nativeClient)

	// Create FUSE filesystem
	fs := fusefs.NewTiramisuFS(cfg.DataDir, client, raCache)

	// Start watched directory
	go WatchDirectory(cfg.DataDir, func(path string) {
		log.Printf("[Watch] Processing new torrent: %s", filepath.Base(path))
		if err := addTorrent(cfg.DataDir, path); err != nil {
			log.Printf("[Watch] Failed to process torrent: %v", err)
		}
	})

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		fs.Unmount()
	}()

	// Mount FUSE and block
	log.Printf("[Tiramisu] Ready — mount at %s", cfg.FuseMountPoint)
	fs.Mount(cfg.FuseMountPoint)
}

func addTorrent(dataDir, torrentPath string) error {
	absPath, err := filepath.Abs(torrentPath)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}

	stubs, err := stub.CreateStubsFromTorrent(dataDir, absPath)
	if err != nil {
		return fmt.Errorf("create stubs: %w", err)
	}

	for _, s := range stubs {
		log.Printf("Created stub: %s", s)
	}
	return nil
}

func addMagnet(dataDir, magnetURI string) error {
	stubPath, err := stub.CreateStubFromMagnet(dataDir, magnetURI)
	if err != nil {
		return fmt.Errorf("create stub: %w", err)
	}

	log.Printf("Created stub: %s", stubPath)
	return nil
}
