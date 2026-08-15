package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg := LoadConfig("")
	if cfg.MasterConcurrencyLimit != 25 {
		t.Errorf("expected concurrency 25, got %d", cfg.MasterConcurrencyLimit)
	}
	if cfg.ReadAheadBudgetMB != 128 {
		t.Errorf("expected read ahead 128MB, got %d", cfg.ReadAheadBudgetMB)
	}
	if cfg.FuseMountPoint == "" {
		t.Errorf("expected non-empty mount point, got empty")
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "config.json")
	content := `{
		"master_concurrency_limit": 10,
		"read_ahead_budget_mb": 128,
		"fuse_mount_point": "Y:",
		"data_dir": "C:\\Torrents",
		"log_level": "DEBUG"
	}`
	os.WriteFile(configPath, []byte(content), 0644)

	cfg := LoadConfig(configPath)
	if cfg.MasterConcurrencyLimit != 10 {
		t.Errorf("expected concurrency 10, got %d", cfg.MasterConcurrencyLimit)
	}
	if cfg.ReadAheadBudgetMB != 128 {
		t.Errorf("expected read ahead 128MB, got %d", cfg.ReadAheadBudgetMB)
	}
	if cfg.FuseMountPoint != "Y:" {
		t.Errorf("expected mount Y:, got %s", cfg.FuseMountPoint)
	}
	if cfg.DataDir != "C:\\Torrents" {
		t.Errorf("expected data dir C:\\Torrents, got %s", cfg.DataDir)
	}
	if cfg.LogLevel != "DEBUG" {
		t.Errorf("expected log level DEBUG, got %s", cfg.LogLevel)
	}
}

func TestLoadConfigEnvOverride(t *testing.T) {
	os.Setenv("BITTORRENTFS_MOUNT", "W:")
	defer os.Unsetenv("BITTORRENTFS_MOUNT")

	cfg := LoadConfig("")
	if cfg.FuseMountPoint != "W:" {
		t.Errorf("expected mount W: from env, got %s", cfg.FuseMountPoint)
	}
}

func TestDerivedFields(t *testing.T) {
	cfg := LoadConfig("")
	if cfg.ReadAheadBudget < cfg.ReadAheadBase {
		t.Errorf("ReadAheadBudget (%d) < ReadAheadBase (%d)", cfg.ReadAheadBudget, cfg.ReadAheadBase)
	}
	if cfg.MetadataCacheSize < 1*1024*1024 {
		t.Errorf("MetadataCacheSize too small: %d", cfg.MetadataCacheSize)
	}
	if cfg.StreamingThreshold != 128*1024 {
		t.Errorf("expected StreamingThreshold 128KB, got %d", cfg.StreamingThreshold)
	}
}
