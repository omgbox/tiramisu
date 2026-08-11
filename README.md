# tiramisu

FUSE streaming engine for BitTorrent. Mount a virtual drive and stream torrents directly to any media player.

## Requirements

- **Windows 10/11** with [WinFsp](https://github.com/winfsp/winfsp/releases) installed
- No other dependencies — single binary

## Usage

```bash
# Stream a .torrent file
tiramisu.exe "C:\Downloads\movie.torrent"

# Stream from magnet link
tiramisu.exe "magnet:?xt=urn:btih:abc123..."

# Specify mount point
tiramisu.exe --mount Y: "movie.torrent"

# Multiple torrents
tiramisu.exe "movie1.torrent" "movie2.torrent"

# Just mount (watch for .torrent files in data dir)
tiramisu.exe --data-dir "C:\Torrents"
```

## Configuration

Create `config.json` next to the binary:

```json
{
  "master_concurrency_limit": 25,
  "read_ahead_budget_mb": 256,
  "fuse_mount_point": "Z:",
  "data_dir": "data",
  "disk_warmup_quota_gb": 15
}
```

## How it works

1. Parses .torrent file → creates virtual .mkv stubs
2. Mounts FUSE drive at Z:
3. Opens Z:\movie.mkv → triggers torrent download
4. Streams data from swarm → cache → player
5. Drop more .torrent files into data dir → auto-detected

## Building

```bash
# Requires WinFsp SDK installed
set CPATH=C:\Program Files (x86)\WinFsp\inc\fuse
CGO_ENABLED=1 go build -o tiramisu.exe .
```

Or use GitHub Actions CI — push to trigger automated Windows build.

## License

GPL-3.0
