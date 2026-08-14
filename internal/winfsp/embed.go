package winfsp

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

//go:embed winfsp-x64.dll
var winfspDLL []byte

var (
	extractedPath string
	extractOnce   sync.Once
	extractErr    error
)

// Ensure extracts the embedded winfsp-x64.dll next to the executable.
// Windows LoadLibrary searches the exe's directory first, so this is
// sufficient for cgofuse to find the DLL.
func Ensure() (string, error) {
	extractOnce.Do(func() {
		// Get the directory containing this executable
		exePath, err := os.Executable()
		if err != nil {
			extractErr = fmt.Errorf("get executable path: %w", err)
			return
		}
		exeDir := filepath.Dir(exePath)

		// Determine DLL name based on architecture
		dllName := "winfsp-x64.dll"
		if runtime.GOARCH == "386" {
			dllName = "winfsp-x86.dll"
		}

		dllPath := filepath.Join(exeDir, dllName)

		// Check if already extracted (same size = skip write)
		if info, err := os.Stat(dllPath); err == nil && int(info.Size()) == len(winfspDLL) {
			extractedPath = dllPath
			return
		}

		if err := os.WriteFile(dllPath, winfspDLL, 0755); err != nil {
			extractErr = fmt.Errorf("write DLL: %w", err)
			return
		}

		extractedPath = dllPath
	})

	return extractedPath, extractErr
}
