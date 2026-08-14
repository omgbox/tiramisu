# BitTorrentFS

FUSE streaming engine for BitTorrent. Mount a virtual drive and stream torrents directly to any media player — single binary, zero config.

## Features

- **Single binary** — WinFsp DLL embedded, UPX compressed (~6.5MB)
- **FUSE virtual filesystem** — mounts as a drive letter (T:)
- **Adaptive read-ahead** — learns player access patterns, dual-stream prefetch
- **Multi-torrent** — 8+ concurrent streams
- **Auto-detection** — drop `.torrent` files into watch directory, files appear instantly
- **Zero config** — works out of the box

## Requirements

- **Windows 10/11** (WinFsp auto-extracted on first run)

## Download

Download `bittorrentfs.exe` from [Releases](https://github.com/omgbox/tiramisu/releases).

## Usage

```bash
# Stream a .torrent file
bittorrentfs.exe "C:\Downloads\movie.torrent"

# Stream from magnet link
bittorrentfs.exe "magnet:?xt=urn:btih:abc123..."

# Mount and watch a directory for .torrent files
bittorrentfs.exe --data-dir "C:\Downloads\torrents"

# Specify custom mount point
bittorrentfs.exe --mount Z: "movie.torrent"

# Multiple torrents
bittorrentfs.exe "movie1.torrent" "movie2.torrent"
```

## Configuration

Optional `config.json` next to the binary:

```json
{
  "master_concurrency_limit": 25,
  "read_ahead_budget_mb": 128,
  "fuse_mount_point": "T:",
  "data_dir": "C:\\Users\\you\\Downloads\\torrents",
  "disk_warmup_quota_gb": 15
}
```

Environment variables (prefix `BITTORRENTFS_`):
- `BITTORRENTFS_FUSE_MOUNT_POINT=Z:`
- `BITTORRENTFS_DATA_DIR=C:\Torrents`

## How it works

1. Parses `.torrent` file → creates virtual video stubs (`.mkv`, `.mp4`)
2. Mounts FUSE drive at `T:`
3. Opening `T:\movie.mkv` with VLC/mpv triggers torrent download
4. Adaptive pump fetches data ahead of the player position
5. Drop more `.torrent` files into the watch directory → auto-detected

## Building

```bash
# Requires WinFsp SDK
set CPATH=C:\Program Files (x86)\WinFsp\inc\fuse
set CGO_ENABLED=1
go build -tags "disable_libutp,noboltdb" -o bittorrentfs.exe .
```

Or push to trigger GitHub Actions CI (builds + UPX compresses automatically).

## License

GPL-3.0
