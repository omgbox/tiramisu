package stub

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestParseStub(t *testing.T) {
	tmp := t.TempDir()
	stubPath := filepath.Join(tmp, "movie.mkv")

	meta := StubMeta{
		URL:    "http://example.com/stream",
		Size:   1234567890,
		Magnet: "magnet:?xt=urn:btih:abc123def456",
	}

	data, _ := json.MarshalIndent(meta, "", "  ")
	os.WriteFile(stubPath, data, 0644)

	parsed, err := ParseStub(stubPath)
	if err != nil {
		t.Fatalf("ParseStub failed: %v", err)
	}

	if parsed.Size != 1234567890 {
		t.Errorf("expected size 1234567890, got %d", parsed.Size)
	}
	if parsed.Magnet != "magnet:?xt=urn:btih:abc123def456" {
		t.Errorf("expected magnet URI, got %s", parsed.Magnet)
	}
}

func TestParseStubMissingMagnet(t *testing.T) {
	tmp := t.TempDir()
	stubPath := filepath.Join(tmp, "bad.mkv")

	meta := StubMeta{
		URL:  "http://example.com",
		Size: 1000,
	}

	data, _ := json.MarshalIndent(meta, "", "  ")
	os.WriteFile(stubPath, data, 0644)

	_, err := ParseStub(stubPath)
	if err == nil {
		t.Fatal("expected error for stub without magnet")
	}
}

func TestVideoExts(t *testing.T) {
	cases := []struct {
		ext  string
		want bool
	}{
		{".mkv", true},
		{".mp4", true},
		{".avi", true},
		{".mov", true},
		{".txt", false},
		{".nfo", false},
		{".srt", false},
		{".jpg", false},
	}

	for _, c := range cases {
		got := videoExts[c.ext]
		if got != c.want {
			t.Errorf("videoExts[%s] = %v, want %v", c.ext, got, c.want)
		}
	}
}
