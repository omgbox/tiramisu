# BitTorrentFS

FUSE streaming engine for BitTorrent. Mount a virtual drive and stream torrents directly to any media player — single binary, zero config.

## Features

- **Single binary** — WinFsp DLL embedded, UPX compressed (~6.5MB)
- **FUSE virtual filesystem** — mounts as a drive letter (T:)
- **Adaptive read-ahead** — learns player access patterns, dual-stream prefetch
- **Multi-torrent** — 8+ concurrent streams
- **Auto-detection** — drop `.torrent` files into watch directory, files appear instantly
- **Zero config** — works out of the box

## Use Cases

### Instant Media Playback
Stream movies and TV shows directly from torrents without waiting for full downloads. Open a `.torrent` file and start watching in VLC/mpv immediately — the adaptive pump fetches data ahead of playback.

### Media Library Server
Mount a watch directory (`C:\Downloads\torrents`) and drop `.torrent` files into it. Files appear on the virtual drive instantly. Combine with Plex/Jellyfin pointing at the mount for a self-updating media library.

### Batch Download & Preview
Load multiple torrents simultaneously. Browse and preview any file on the virtual drive while others download in the background. Check video quality, subtitle tracks, or audio streams before committing to a full download.

### CI/CD Media Processing
Automate video transcoding pipelines. Point your transcoder at the FUSE mount — it sees files as soon as metadata arrives from the swarm. No intermediate storage needed.

### Ephemeral Workspace
Use as a disposable filesystem for testing torrent clients, validating `.torrent` files, or demonstrating BitTorrent protocols without touching disk.

### Portable Streaming
Single 6.5MB binary with no installation. Copy to a USB drive, run on any Windows machine. WinFsp DLL auto-extracts on first run.

## Requirements

- **Windows 10/11** (WinFsp auto-extracted on first run)

## Download

Download `bittorrentfs.exe` (6.5MB) from [Releases](https://github.com/omgbox/tiramisu/releases).

## Usage

```bash
# Stream a .torrent file directly
bittorrentfs.exe "C:\Downloads\movie.torrent"
# → Opens T:\movie.mkv — play in VLC/mpv

# Watch a folder: drop .torrents, files appear instantly
bittorrentfs.exe --data-dir "C:\Downloads\torrents"
# → T:\ is mounted. Any .torrent you drop in that folder
#   instantly creates a virtual file on T:\

# Watch folder + magnet link
bittorrentfs.exe --data-dir "C:\Downloads\torrents" "magnet:?xt=urn:btih:abc123..."

# Multiple torrents at once
bittorrentfs.exe "movie1.torrent" "movie2.torrent" "show.s01e01.torrent"

# Custom mount point
bittorrentfs.exe --mount Z: "movie.torrent"

# Specify download location for torrent data
bittorrentfs.exe --download-dir "D:\Cache" "movie.torrent"
```

**Typical workflow:**
1. Run `bittorrentfs.exe --data-dir "C:\Downloads\torrents"`
2. Mount appears at `T:`
3. Drop `.torrent` files into `C:\Downloads\torrents`
4. Files appear on `T:\` instantly
5. Open any `.mkv`/`.mp4` in VLC — streaming starts immediately
6. Eject with Ctrl+C — cache is retained for next session

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

## Credits

Derived from [MrRobotoGit/tiramisu](https://github.com/MrRobotoGit/tiramisu) — a BitTorrent streaming engine for Windows.

Additional dependencies:

| Project | Purpose |
|---------|---------|
| [WinFsp](https://github.com/winfsp/winfsp) + [cgofuse](https://github.com/winfsp/cgofuse) | Windows FUSE filesystem layer |
| [anacrolix/torrent](https://github.com/anacrolix/torrent) | BitTorrent protocol implementation (local fork) |
| [anacrolix/dht](https://github.com/anacrolix/dht/v2) | Distributed Hash Table for peer discovery |
| [fsnotify](https://github.com/fsnotify/fsnotify) | Filesystem event notifications |
| [bbolt](https://go.etcd.io/bbolt) | Embedded key/value database |
| [UPX](https://github.com/upx/upx) | Executable compression |

## License

GPL-3.0
