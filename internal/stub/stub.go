package stub

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/anacrolix/torrent/metainfo"
)

// StubMeta represents the metadata stored in a .mkv stub file.
type StubMeta struct {
	URL      string `json:"url"`
	Size     int64  `json:"size"`
	Magnet   string `json:"magnet"`
	FilePath string `json:"file_path,omitempty"` // path within the torrent (e.g. "video/Sintel.mkv")
}

// Video extensions that get stubs.
var videoExts = map[string]bool{
	".mkv":  true,
	".mp4":  true,
	".avi":  true,
	".mov":  true,
	".wmv":  true,
	".flv":  true,
	".webm": true,
}

// openForRead opens a file for reading. On Windows, uses FILE_SHARE_DELETE
// so the file can be deleted while we hold it open (required for auto-cleanup).
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

// CreateStubsFromTorrent parses a .torrent file and creates .mkv stubs in dataDir.
// Returns list of created stub paths.
func CreateStubsFromTorrent(dataDir, torrentPath string) ([]string, error) {
	f, err := openForRead(torrentPath)
	if err != nil {
		return nil, fmt.Errorf("open torrent: %w", err)
	}
	defer f.Close()

	mi, err := metainfo.Load(f)
	if err != nil {
		return nil, fmt.Errorf("load torrent: %w", err)
	}

	info, err := mi.UnmarshalInfo()
	if err != nil {
		return nil, fmt.Errorf("unmarshal info: %w", err)
	}

	hash := mi.HashInfoBytes()
	hashHex := fmt.Sprintf("%x", hash)

	files := info.UpvertedFiles()
	var stubs []string

	for _, f := range files {
		path := f.BestPath()
		fullPath := strings.Join(path, "/") // torrent paths always use forward slashes
		// Single-file torrents return empty path from BestPath()
		if fullPath == "" {
			fullPath = info.Name
		}
		ext := filepath.Ext(fullPath)

		if !videoExts[ext] {
			continue
		}

		// Create subdirectory if needed
		stubDir := filepath.Join(dataDir, info.BestName())
		os.MkdirAll(stubDir, 0755)

		// Create stub file
		stubName := filepath.Base(fullPath)
		stubPath := filepath.Join(stubDir, stubName)

		magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s", hashHex)
		if info.Name != "" {
			magnet += "&dn=" + info.BestName()
		}

		meta := StubMeta{
			URL:      "",
			Size:     f.Length,
			Magnet:   magnet,
			FilePath: fullPath,
		}

		data, err := json.MarshalIndent(meta, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal stub: %w", err)
		}

		if err := os.WriteFile(stubPath, data, 0644); err != nil {
			return nil, fmt.Errorf("write stub: %w", err)
		}

		stubs = append(stubs, stubPath)
	}

	return stubs, nil
}

// CreateStubFromMagnet creates a stub from a magnet link.
// Size is unknown (set to 0) until metadata arrives.
// Returns the stub path.
func CreateStubFromMagnet(dataDir, magnetURI string) (string, error) {
	parsed, err := metainfo.ParseMagnetUri(magnetURI)
	if err != nil {
		return "", fmt.Errorf("parse magnet: %w", err)
	}

	hashHex := fmt.Sprintf("%x", parsed.InfoHash)

	// Use display name or hash as directory name
	name := parsed.DisplayName
	if name == "" {
		name = hashHex
	}

	stubDir := filepath.Join(dataDir, name)
	os.MkdirAll(stubDir, 0755)

	stubName := name + ".mkv"
	stubPath := filepath.Join(stubDir, stubName)

	meta := StubMeta{
		URL:    "",
		Size:   0, // Unknown until metadata
		Magnet: magnetURI,
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal stub: %w", err)
	}

	if err := os.WriteFile(stubPath, data, 0644); err != nil {
		return "", fmt.Errorf("write stub: %w", err)
	}

	return stubPath, nil
}

// ParseStub reads a .mkv stub file and returns its metadata.
func ParseStub(stubPath string) (*StubMeta, error) {
	data, err := os.ReadFile(stubPath)
	if err != nil {
		return nil, fmt.Errorf("read stub: %w", err)
	}

	var meta StubMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("parse stub: %w", err)
	}

	if meta.Magnet == "" {
		return nil, fmt.Errorf("stub missing magnet URI")
	}

	return &meta, nil
}

// ExtractHash extracts the info hash hex string from a magnet URI.
func ExtractHash(magnetURI string) string {
	parsed, err := metainfo.ParseMagnetUri(magnetURI)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", parsed.InfoHash)
}

// RemoveStubs scans dataDir for .mkv stubs whose magnet hash matches hashHex,
// removes them, and cleans up empty parent directories.
// Returns the number of stubs removed.
func RemoveStubs(dataDir, hashHex string) int {
	removed := 0

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return 0
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		stubDir := filepath.Join(dataDir, e.Name())
		stubFiles, readErr := os.ReadDir(stubDir)
		if readErr != nil {
			continue
		}
		for _, sf := range stubFiles {
			if sf.IsDir() || !strings.HasSuffix(sf.Name(), ".mkv") {
				continue
			}
			meta, parseErr := ParseStub(filepath.Join(stubDir, sf.Name()))
			if parseErr != nil {
				continue
			}
			h := ExtractHash(meta.Magnet)
			if h == hashHex {
				os.Remove(filepath.Join(stubDir, sf.Name()))
				removed++
			}
		}
		// Remove empty stub directories
		if remaining, _ := os.ReadDir(stubDir); len(remaining) == 0 {
			os.Remove(stubDir)
		}
	}

	return removed
}
