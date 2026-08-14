package main

import (
	"fmt"
	"bittorrentfs/cmd"
)

// Set via -ldflags "-X main.version=..." at build time.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Printf("[main] BitTorrentFS %s (commit=%s, built=%s) starting...\n", version, commit, date)
	cmd.Execute()
}
