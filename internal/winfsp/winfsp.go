package winfsp

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	winfspMSIURL    = "https://github.com/winfsp/winfsp/releases/download/v2.1/winfsp-2.1.25156.msi"
	winfspMSIName   = "winfsp-2.1.25156.msi"
	winfspMinVersion = "2.1"
)

// IsInstalled checks if WinFsp kernel driver is installed and loaded.
func IsInstalled() bool {
	// Look for fsptool in WinFsp installation directory
	programFiles := os.Getenv("ProgramFiles(x86)")
	if programFiles == "" {
		programFiles = `C:\Program Files (x86)`
	}
	
	fsptool := filepath.Join(programFiles, "WinFsp", "bin", "fsptool-x64.exe")
	if _, err := os.Stat(fsptool); os.IsNotExist(err) {
		// Also check system32
		fsptool = filepath.Join(os.Getenv("SystemRoot"), "System32", "fsptool-x64.exe")
		if _, err := os.Stat(fsptool); os.IsNotExist(err) {
			return false
		}
	}
	
	// Run fsptool lsdrv to check for loaded driver
	cmd := exec.Command(fsptool, "lsdrv")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}
	
	// If output contains any driver name, WinFsp is installed
	return len(output) > 0
}

// Ensure checks if WinFsp is installed, downloads and installs if needed.
func Ensure() error {
	if IsInstalled() {
		return nil
	}
	
	fmt.Println("[BitTorrentFS] WinFsp not found. Installing automatically...")
	
	// Download MSI installer
	tmpDir := os.TempDir()
	msiPath := filepath.Join(tmpDir, winfspMSIName)
	
	if err := downloadFile(msiPath, winfspMSIURL); err != nil {
		return fmt.Errorf("download WinFsp: %w", err)
	}
	defer os.Remove(msiPath)
	
	// Silent install with msiexec
	fmt.Println("[BitTorrentFS] Installing WinFsp (this may take a moment)...")
	cmd := exec.Command("msiexec.exe", "/i", msiPath, "/quiet", "/norestart")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("install WinFsp: %w", err)
	}
	
	// Wait for driver to load
	time.Sleep(2 * time.Second)
	
	// Verify installation
	if !IsInstalled() {
		return fmt.Errorf("WinFsp installation completed but driver not detected")
	}
	
	fmt.Println("[BitTorrentFS] WinFsp installed successfully")
	return nil
}

// downloadFile downloads a file from URL to local path.
func downloadFile(filepath string, url string) error {
	fmt.Printf("[BitTorrentFS] Downloading WinFsp installer...\n")
	
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}
	
	out, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer out.Close()
	
	// Copy with progress
	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	
	fmt.Printf("[BitTorrentFS] Downloaded %.1MB\n", float64(written)/1024/1024)
	return nil
}
