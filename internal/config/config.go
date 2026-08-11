package config

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Config holds all configurable parameters for the FUSE streaming engine.
type Config struct {
	// Core tuning
	MasterConcurrencyLimit int    `json:"master_concurrency_limit"`
	ReadAheadBudgetMB      int64  `json:"read_ahead_budget_mb"`
	FuseBlockSize          int    `json:"fuse_block_size_bytes"`
	StreamingThresholdKB   int64  `json:"streaming_threshold_kb"`
	LogLevel               string `json:"log_level"`

	// FUSE timing
	AttrTimeoutSeconds     float64 `json:"attr_timeout_seconds"`
	EntryTimeoutSeconds    float64 `json:"entry_timeout_seconds"`
	NegativeTimeoutSeconds float64 `json:"negative_timeout_seconds"`

	// Paths
	PhysicalSourcePath string `json:"physical_source_path"`
	FuseMountPoint     string `json:"fuse_mount_point"`
	DataDir            string `json:"data_dir"`

	// Cache
	MetadataCacheSizeMB int64 `json:"metadata_cache_size_mb"`
	DiskWarmupQuotaGB   int64 `json:"disk_warmup_quota_gb"`

	// Network
	GoStormBaseURL string `json:"gostorm_url"`

	// Warmup
	WarmupHeadSizeMB int64 `json:"warmup_head_size_mb"`

	// Derived fields (calculated from JSON, not from JSON directly)
	ReadAheadBudget    int64         `json:"-"`
	MetadataCacheSize  int64         `json:"-"`
	StreamingThreshold int64         `json:"-"`
	ReadAheadBase      int64         `json:"-"`
	PreloadWorkers     int           `json:"-"`
	MaxConcurrentHTTP  int           `json:"-"`
	KeepaliveInterval  time.Duration `json:"-"`
	CacheTTL           time.Duration `json:"-"`
	ConfigPath         string        `json:"-"`
	RootPath           string        `json:"-"`
}

// LoadConfig loads configuration from a JSON file with defaults.
func LoadConfig(configPath string) Config {
	cfg := Config{
		// Core
		MasterConcurrencyLimit: 25,
		ReadAheadBudgetMB:      256,
		FuseBlockSize:          1048576,
		StreamingThresholdKB:   128,
		LogLevel:               "INFO",

		// FUSE
		AttrTimeoutSeconds:     1.0,
		EntryTimeoutSeconds:    1.0,
		NegativeTimeoutSeconds: 0.0,

		// Paths
		PhysicalSourcePath: "data",
		FuseMountPoint:     "Z:",
		DataDir:            "data",

		// Cache
		MetadataCacheSizeMB: 50,
		DiskWarmupQuotaGB:   15,

		// Network
		GoStormBaseURL: "http://127.0.0.1:8090",

		// Warmup
		WarmupHeadSizeMB: 64,

		// Derived defaults
		ReadAheadBase:      16 * 1024 * 1024,
		PreloadWorkers:     4,
		MaxConcurrentHTTP:  25,
		KeepaliveInterval:  15 * time.Second,
		CacheTTL:           10 * time.Second,
	}

	// Resolve config path
	if configPath == "" {
		if p := os.Getenv("TIRAMISU_CONFIG"); p != "" {
			configPath = p
		} else {
			exe, err := os.Executable()
			if err == nil {
				configPath = filepath.Join(filepath.Dir(exe), "config.json")
			} else {
				configPath = "config.json"
			}
		}
	}
	cfg.ConfigPath = configPath

	// Try to load JSON
	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Printf("[Config] WARNING: Failed to parse %s: %v", configPath, err)
		} else {
			log.Printf("[Config] Loaded settings from %s", configPath)
		}
	}

	// Override from environment
	cfg.applyEnvOverrides()

	// Finalize derived fields
	cfg.finalize()

	return cfg
}

// applyEnvOverrides applies environment variable overrides (highest priority).
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("TIRAMISU_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MasterConcurrencyLimit = n
		}
	}
	if v := os.Getenv("TIRAMISU_READ_AHEAD_BUDGET"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			c.ReadAheadBudgetMB = n / (1024 * 1024)
		}
	}
	if v := os.Getenv("TIRAMISU_GOSTORM_URL"); v != "" {
		c.GoStormBaseURL = v
	}
	if v := os.Getenv("TIRAMISU_LOG_LEVEL"); v != "" {
		c.LogLevel = strings.ToUpper(v)
	}
	if v := os.Getenv("TIRAMISU_MOUNT"); v != "" {
		c.FuseMountPoint = v
	}
	if v := os.Getenv("TIRAMISU_DATA_DIR"); v != "" {
		c.DataDir = v
	}
}

// finalize maps JSON fields to internal logic fields.
func (c *Config) finalize() {
	c.MaxConcurrentHTTP = c.MasterConcurrencyLimit

	// Calculate ReadAheadBudget in bytes
	c.ReadAheadBudget = c.ReadAheadBudgetMB * 1024 * 1024
	if c.ReadAheadBudget < 10*1024*1024 {
		c.ReadAheadBudget = 10 * 1024 * 1024 // Min 10MB
	}
	if c.ReadAheadBudget < c.ReadAheadBase {
		c.ReadAheadBudget = c.ReadAheadBase
	}

	// Calculate MetadataCacheSize in bytes
	c.MetadataCacheSize = c.MetadataCacheSizeMB * 1024 * 1024
	if c.MetadataCacheSize < 1*1024*1024 {
		c.MetadataCacheSize = 1 * 1024 * 1024 // Min 1MB
	}

	// StreamingThreshold
	c.StreamingThreshold = c.StreamingThresholdKB * 1024

	// Resolve root path
	if runtime.GOOS == "windows" {
		appData := os.Getenv("LOCALAPPDATA")
		if appData == "" {
			appData = os.Getenv("APPDATA")
		}
		c.RootPath = filepath.Join(appData, "tiramisu")
	} else {
		home, _ := os.UserHomeDir()
		c.RootPath = filepath.Join(home, "tiramisu")
	}

	// Ensure data directory path is absolute
	if !filepath.IsAbs(c.DataDir) {
		exe, err := os.Executable()
		if err == nil {
			c.DataDir = filepath.Join(filepath.Dir(exe), c.DataDir)
		}
	}
}

// LogConfig logs the active configuration.
func (c *Config) LogConfig() {
	log.Printf("=== Configuration ===")
	log.Printf("Config: %s", c.ConfigPath)
	log.Printf("Root: %s", c.RootPath)
	log.Printf("DataDir: %s", c.DataDir)
	log.Printf("MountPoint: %s", c.FuseMountPoint)
	log.Printf("Concurrency: %d", c.MasterConcurrencyLimit)
	log.Printf("ReadAheadBudget: %d MB", c.ReadAheadBudgetMB)
	log.Printf("CacheSize: %d MB", c.MetadataCacheSizeMB)
	log.Printf("WarmupQuota: %d GB", c.DiskWarmupQuotaGB)
	log.Printf("LogLevel: %s", c.LogLevel)
	log.Printf("=====================")
}

// Save persists the current configuration to its file.
func (c *Config) Save() error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.ConfigPath, data, 0644)
}
