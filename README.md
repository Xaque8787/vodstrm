<p align="center">
  <img src="app/static/vodstrm_images/vodstrm_1024.png" alt="VODSTRM" width="600" />
</p>

# VODSTRM

VODSTRM is a self-hosted media library manager that ingests M3U and Xtream Codes playlists, organizes the content into a structured `.strm` file library, and keeps everything in sync automatically. Point it at your IPTV providers, apply filters to clean up titles, and let your media server (Jellyfin, Plex, Emby, etc.) pick up the rest.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Quick Start](#quick-start)
  - [Configuration](#configuration)
    - [.env (Application settings)](#env-application-settings)
- [Usage](#usage)
  - [First Run & Setup](#first-run--setup)
  - [Providers](#providers)
  - [Library](#library)
  - [Live TV](#live-tv)
  - [Filters](#filters)
  - [Schedules](#schedules)
  - [Admin](#admin)
- [Integrations](#integrations)
  - [TMDB](#tmdb)
  - [Downloads](#downloads)

---

## Features

- Supports M3U URL, Xtream Codes API, and local `.m3u` file providers
- Ingests and normalizes VOD content across multiple providers with priority-based deduplication
- Generates `.strm` files organized into Movies, Series, TV VOD, Live, and Unsorted categories
- Flexible filtering system — clean up messy titles, exclude unwanted content, or whitelist specific entries
- Automatic scheduling for downloads, ingestion, and library sync
- Follow rules to automatically add new episodes/seasons as they appear — in STRM mode (generate `.strm` files) or Download mode (fetch and store local media files)
- Optional media downloads — convert remote streams to local files via ffmpeg, with a download queue, retry, and retention management
- Per-provider STRM mode: generate everything or only import what you select
- Multi-user support with JWT-based authentication

---

## Installation

### Prerequisites

- Docker and Docker Compose
- A media server that supports `.strm` files (Jellyfin, Plex, Emby, Kodi, etc.)
- ffmpeg — included in the Docker image. If running outside Docker, install `ffmpeg` and `ffprobe` system-wide (used by the optional Downloads feature)

### Quick Start

1. Create a directory for VODSTRM and place your `docker-compose.yml` and `.env` files inside it.

2. Copy the following into `docker-compose.yml`:

```yaml
services:
  vodstrm:
    image: ghcr.io/xaque8787/vodstrm:latest
    container_name: vodstrm
    restart: unless-stopped
    ports:
      - "${APP_PORT}:${APP_PORT}"
    env_file:
      - .env
    volumes:
      - ${DATA_PATH}:/app/data
      # Uncomment and point to a host path containing local .m3u files. Or place m3u files directly in /app/data/m3u.
      # - /path/on/host/to/m3u:/app/data/m3u
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:${APP_PORT}/login"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

3. Create your `.env` file (see [Configuration](#configuration) below).

4. Start the container:

```bash
docker compose up -d
```

5. Open `http://your-host:2112` in a browser to complete the setup wizard.

---

### Configuration

VODSTRM uses one project-root `.env` file for standalone execution, application settings, and Docker Compose interpolation. Copy `example.env` to `.env`, then adjust values for your environment. Python falls back to the checked-in `example.env` when `.env` is absent; Docker Compose requires `.env` for path and port interpolation.

#### .env (Application settings)

This file controls the Docker container itself — ports, paths, timezone, and secrets. Docker Compose reads it automatically when it exists in the same directory as `docker-compose.yml`.

```env
# Standalone server
APP_HOST=0.0.0.0
APP_PORT=2112
APP_RELOAD=true
DEBUG=false

# Authentication
SECRET_KEY=change-me-to-a-random-key
ACCESS_TOKEN_EXPIRE_MINUTES=60
SECURE_COOKIES=false

# Application storage
DATABASE_PATH=data/app.db
SCHEDULER_DB_PATH=data/scheduler.db
VOD_PATH=data/vod
VOD_MOVIES_FOLDER=movies
VOD_SERIES_FOLDER=series
VOD_LIVE_TV_FOLDER=livetv
VOD_UNSORTED_FOLDER=unsorted
VOD_UNKNOWN_YEAR_FOLDER=unknown
M3U_DIR=data/m3u
VOD_OFFLINE_PATH=data/vod-offline
LOG_DIR=data/logs

# Scheduling
TZ=America/Los_Angeles

# Docker Compose
PUID=1000
PGID=1000
DATA_PATH=./data
```

`VOD_PATH` is the media-server-facing tree. `VOD_OFFLINE_PATH` stores the physical downloaded files and mirrors the same relative layout under `movies/`, `series/`, and `unsorted/`. Completed files in `VOD_PATH` are hard links to files in `VOD_OFFLINE_PATH`.

The immediate subfolder names are configurable with `VOD_MOVIES_FOLDER`, `VOD_SERIES_FOLDER`, `VOD_LIVE_TV_FOLDER`, and `VOD_UNSORTED_FOLDER`. Their defaults preserve the existing `movies`, `series`, `livetv`, and `unsorted` layout. Dated TV content shares `VOD_SERIES_FOLDER`; `VOD_UNKNOWN_YEAR_FOLDER` defaults to `unknown` when dated TV has no year. Season directories retain the standard `Season NN` format. Configurable folder values must resolve to one folder name; separators and `..` are rejected.

Both application paths must be on the same filesystem because hard links cannot cross filesystems. Docker therefore mounts only `DATA_PATH` at `/app/data`; keep both paths beneath that mount unless you provide another single common-parent mount yourself.

**Local M3U files:** If you have `.m3u` files on your host, you can either mount a directory into the container (uncomment the volume in `docker-compose.yml` and set the host path), or simply drop the files into `DATA_PATH/m3u` and add them as a Local File provider through the UI.

---

## Usage

### First Run & Setup

On first launch, VODSTRM detects that no admin account exists and redirects you to the setup page at `/setup`. Enter a username and password to create the initial admin account. You will then be redirected to the login page.

---

### Providers

**URL:** `/providers`

The Providers page is where you connect VODSTRM to your playlist sources. Three provider types are supported.

#### M3U URL

Provide a direct HTTP/HTTPS link to an `.m3u` or `.m3u8` file. VODSTRM will download it on a schedule and ingest the contents.

#### Xtream Codes

Enter your Xtream Codes server URL, username, and password. Optionally specify a port and choose a stream format (TS or HLS). VODSTRM first requests the provider's M3U Plus playlist. If playlist export is unavailable, it automatically falls back to the native Xtream Player API.

Native Player API ingestion imports live channels and movies immediately and stores the complete TV-series summary catalogue. Provider synchronization never requests per-series episode details. When a user opens a show in the Library, VODSTRM requests `get_series_info` for that show, persists its episodes, and reuses those on-demand details during later synchronizations.

#### Local File

Select a `.m3u` file already present on the host (mounted into the container). Use the built-in file browser to navigate to the file rather than typing a path manually.

---

#### Provider Settings

Each provider has the following options available after creation:

- **Priority** — When multiple providers supply the same content, the provider with the lowest priority number wins. If two providers tie, they are broken alphabetically by their internal slug. This determines which URL ends up inside the `.strm` file.
- **Quality Terms** — An ordered list of plain-text terms used to score incoming streams before deciding whether they should overwrite an existing stream row for the same content. When quality terms are configured, each ingest run compares the incoming stream's raw title against the existing one: the stream whose title contains more matching terms wins. If the scores are equal the existing stream is kept. If no quality terms are configured the new stream always overwrites the existing one (the original behaviour).

  Terms are matched as whole words and are case-insensitive, so `hd` will not match `uhd` or `hdr`. The order of the list does not affect scoring — each term that appears in the title counts as one point regardless of position in the list.

  **Example:** With quality terms `["4k", "2160p", "1080p", "hd"]`, a stream titled `Movie Title 4K HDR` scores 2 (`4k` and `hd`) while one titled `Movie Title HD` scores 1. The 4K stream wins and its URL is written to the `.strm` file. On the next run, if the 4K version disappears from the provider's playlist entirely, the incoming lower-quality stream automatically wins because the existing row is from a previous run — stale rows never block a live incoming stream.
- **Force VOD content** — Skips live-TV detection during ingestion and classifies every entry from this provider as VOD (series, TV VOD, movie, or unsorted). Useful for providers that tag VOD content with live-style duration values. Enabling or disabling this only takes effect on the next ingest run.
- **Active toggle** — Shows the current state. Active providers show a green toggle; inactive providers show a grey toggle that you can click to re-enable them.
- **Disable** — Clicking the active toggle on a live provider opens a confirmation modal before proceeding. Confirming will mark the provider inactive, immediately remove all of its streams and entries from the database, and hand its owned `.strm` files over to the next eligible provider (or delete them if no alternative exists). This is a destructive operation — data can only be restored by re-enabling the provider and running a fresh ingest.
- **Edit** — Update connection details (URL, credentials, format, file path) at any time.
- **Delete** — Permanently removes the provider and all associated stream records. Owned `.strm` files are either handed to another provider or deleted.

---

#### STRM Modes

Each provider operates in one of two modes, configurable from the Schedules page:

- **Generate All** — Every non-excluded stream from this provider automatically creates a `.strm` file. This is the default and is best for providers where you want everything.
- **Import Selected** — `.strm` files are only created for streams you have manually added through the Library page. Use this for providers with large, noisy playlists where you only want specific titles.

---

### Library

**URL:** `/library`

The Library page is where you browse all ingested content and manage what ends up in your `.strm` output directory.

The Library page is where you browse all ingested content and manage what ends up in your `.strm` output directory.

#### Content Types

Use the tabs at the top to filter by type:

- **All** — Everything across all types.
- **Movies** — Individual movie entries.
- **Series** — Grouped by show title, with drill-down to seasons and episodes.
- **Live** — Live TV channels.
- **TV VOD** — VOD recordings of TV shows, grouped by title and organized by year.
- **Unsorted** — Entries that did not match any recognized naming pattern.

#### Search & Filtering

- Use the search bar to filter entries by title in real time.
- Use the ownership filter to show only entries already in your library, only entries not yet added, or everything.

#### Stream Links

Movie details and individual TV episodes expose their provider stream link with **Reveal** and **Copy** controls. Stream URLs can contain provider credentials, so reveal or copy them only in a private environment.

#### Adding & Removing Content

Depending on the content type, you can add or remove content at different granularities:

- **Individual entry** — Add or remove a single movie, episode, or channel.
- **Season** — Add or remove an entire season of a series at once.
- **Full series** — Add or remove every season and episode for a show.
- **Year** — Add or remove all TV VOD episodes from a specific year.
- **Full TV VOD show** — Add or remove all years for a TV VOD title.

Adding an entry creates the corresponding `.strm` file immediately. Removing it deletes the file.

> This granularity only applies to providers running in **Import Selected** mode. Providers in **Generate All** mode manage their own files automatically.

#### Follow Rules

Follow rules tell VODSTRM to automatically add new content to your library as it appears in future ingestion runs. You can create a follow rule from the same add/remove controls — look for the follow option alongside each title, season, or series.

- **Series follow** — Automatically import every new episode across all future seasons.
- **Season follow** — Automatically import new episodes within a specific season only.
- **TV VOD show follow** — Automatically import all future years for a TV VOD title.
- **TV VOD year follow** — Automatically import new entries for a specific year only.

Follow rules can be reviewed and deleted from the follows management panel on the Library page.

#### Follow Modes

Each follow rule operates in one of two modes:

- **STRM mode** (default) — Matching content is marked as imported and `.strm` files are generated pointing to the remote stream URL. This is the standard behaviour — your media server reads the URL from the `.strm` file and streams directly from the provider.
- **Download mode** — Matching content is queued for local download instead. The download processor fetches the stream via ffmpeg and stores the resulting media file in your VOD directory. Once the download completes, the `.strm` file is removed and your media server reads the local file directly. This is useful for content you want available offline or from providers with unreliable streaming.

---

### Live TV

VODSTRM generates M3U playlist files for your live TV channels alongside the `.strm` library. These are written to the `VOD_LIVE_TV_FOLDER` subdirectory inside your VOD output path (by default, `data/vod/livetv/`).

#### Per-Provider M3U

Each active provider gets its own `.m3u` file named after the provider slug (e.g. `myprovider.m3u`). The contents depend on the provider's STRM mode:

- **Generate All** — All non-excluded live streams from that provider are written.
- **Import Selected** — Only live streams you have manually added through the Library page are written. This lets you curate a trimmed live-TV playlist from a large provider.

#### Combined M3U

A `all_providers.m3u` file is also generated as the union of eligible live streams across every active provider (both modes, respecting imported status for Import Selected). Streams from multiple providers for the same channel are all included — deduplication is left to your IPTV player.

#### Adding Live Channels

To add live channels to an Import Selected provider's M3U, go to the Library page, filter by the **Live** content type, and add the channels you want. The M3U files are regenerated on the next sync and will include your selections.

#### Automatic Maintenance

The M3U files are rewritten at the end of every ingestion run. When a provider is deactivated or deleted, its per-provider `.m3u` file is removed and the combined file is regenerated to reflect the change.

---

### Filters

**URL:** `/filters`

The Filters page lets you define rules that transform and curate stream titles before they are written to the database as `filtered_title`. The filtered title is what gets used for `.strm` file naming and library organization.

#### Filter Types

- **Replace** — Substitute a matched term with a replacement string. Useful for correcting typos, removing provider tags embedded in titles, or normalizing naming conventions.
- **Remove** — Strip matching terms from a title entirely. Good for removing prefixes, suffixes, or bracketed junk that providers insert into stream names.
- **Exclude** — Drop any stream whose title matches the pattern. The stream is still ingested into the database but will never produce a `.strm` file.
- **Include Only** — Whitelist mode. Only streams whose titles match at least one pattern are kept. All others are excluded.

#### Patterns

Each filter supports multiple patterns. Patterns use regular expression syntax. For Replace filters, each pattern also has a corresponding replacement value.

#### Scope

Filters can be scoped to limit where they apply:

- **Providers** — Restrict a filter to one or more specific providers. Leave blank to apply to all providers.
- **Entry Types** — Restrict a filter to specific content types (movies, series, live, etc.). Leave blank to apply to all types.

#### Execution Order

Filters run in the order defined by their order index. The pipeline for each stream runs as: Replace → Remove → Normalize whitespace → Exclude → Include Only. You can reorder filters to control precedence.

#### Reapply Filters

The **Reapply All Filters** button re-runs the entire filter pipeline against every stream currently in the database. Use this after creating or editing filters to update existing content without waiting for the next scheduled download.

---

### Schedules

**URL:** `/schedules`

The Schedules page controls when VODSTRM automatically fetches and processes your providers. It also displays live per-provider progress, durable stream/STRM counts, series-cache progress, and the resolved output directory.

Provider synchronization runs in phases: catalogue retrieval, database persistence, filtering, STRM generation, and live M3U generation. Files do not appear in the output directory during the earlier API and database phases. The live monitor identifies the active phase and begins reporting STRM counts as file generation completes.

#### Global Tasks

Three global tasks are available:

- **Sync All Active Providers** — Retrieves the latest provider catalogues, runs ingestion, applies filters, and synchronizes `.strm` and live `.m3u` files. This is the primary task you will want to run on a regular schedule.
- **Clean STRM Orphans** — Scans the VOD output directory and removes any `.strm` files that no longer have a corresponding database entry. Useful for cleaning up after providers are deleted or content is removed.
- **Process Download Queue** — Wakes the download processor, which claims pending download rows up to the configured concurrency limit, probes each stream with ffprobe, and downloads via ffmpeg stream-copy remux. Completed downloads trigger a STRM sync so that `.strm` files are cleaned up for entries that now have local files.

Each global task can be:
- **Sync Now / Run Now** — Starts the selected provider sync or maintenance task immediately in the background.
- **Scheduled** — Set a recurring trigger using either a cron expression (e.g. `0 3 * * *` for 3 AM daily) or a simple interval (e.g. every 6 hours).

#### Per-Provider Controls

The provider table below the global tasks shows each configured provider with the following controls:

- **Omit from Schedule** — Temporarily exclude a provider from all scheduled and global ingest runs without touching its data. When omitted, the provider's existing streams and entries remain in the database exactly as they are — they just will not be updated. An amber **Omitted** badge appears next to the provider name as a reminder that its data may be stale. Click the toggle again to include the provider in future runs. To manually run an omitted provider regardless, use **Sync Now**. To disable a provider and remove its data, use the Disable action on the Providers page.
- **Sync Now** — Manually synchronize one provider without affecting others. Works even if the provider is omitted from the schedule. Duplicate clicks are ignored while that provider is already running.
- **STRM Mode** — Toggle between Generate All and Import Selected mode for the provider.

---

### Admin

The Admin section contains tools for inspecting the internal state of the application and managing users. It is intended for troubleshooting rather than day-to-day use.

#### Users

**URL:** `/admin/users`

Lists all user accounts. Any user can be deleted except the currently logged-in account. VODSTRM does not currently support self-service registration — new accounts must be created by an existing admin.

#### Database

**URL:** `/admin/database`

A low-level view into the entries and streams tables in the database.

- **Entries tab** — Shows every ingested media entry. Supports search, sorting, and pagination (100 per page). Useful for verifying that ingestion is working and titles are being parsed correctly.
- **Streams tab** — Shows the individual stream records attached to each entry, including which provider they came from, their current URL, and their `filtered_title`. Expand any row to inspect the raw metadata JSON and the filter hits that were applied during the last filter run.

The **Clear Entries** and **Clear Streams** buttons wipe the respective tables. Use with caution — clearing entries will remove all ownership and follow data, and the next ingestion run will treat everything as new.

#### Logs

**URL:** `/admin/logs`

Application logs are written persistently to `LOG_DIR/app.log` (default `data/logs/app.log`). Docker stores that directory under the `${DATA_PATH}:/app/data` volume. The log rotates at 5 MB and retains three archives: `app.log.1`, `app.log.2`, and `app.log.3`.

The admin-only viewer can switch between current and rotated files, filter by severity, search logger names or messages, change the tail limit, and auto-refresh the current view. Severity is color-coded: debug, informational, warning, error, and critical records each have distinct visual treatment. The viewer only exposes the known `app.log` rotation family and does not accept arbitrary filesystem paths.

---

## Integrations

**URL:** `/integrations`

Integrations connect VODSTRM to external services for metadata enrichment and library management. Each integration is independently configured and runs asynchronously in the background — the core ingest pipeline is never slowed down by external API calls.

### TMDB

The TMDB (The Movie Database) integration enriches your ingested content with metadata fetched from the TMDB API. After each ingest run, new movies and series entries are queued for lookup. A background processor then resolves them against the TMDB API in a rate-limited, best-effort manner, writing cover art URLs and matched TMDB IDs back to the database.

To enable it:

1. Register for a free account at [themoviedb.org](https://www.themoviedb.org) and generate an API key from your account settings.
2. Open the Integrations page, enter your API key in the TMDB settings block, and enable the integration.

The status widget on the Integrations page shows the current queue depth, the last time the processor ran, and how many items have completed or failed. Failed lookups are retained so you can see what did not resolve — they will not block the rest of the queue.

### Downloads

The Downloads integration converts remote stream URLs into persistent offline media. The physical file is stored under `VOD_OFFLINE_PATH`; a hard link with the same relative path is created under `VOD_PATH` so your media server can read it directly. This is useful for offline access, unreliable providers, or content you want to keep permanently.

#### How It Works

1. **Queuing** — Downloads are queued in two ways:
   - **Manual** — Click the Download button on any entry, season, series, or TV VOD year in the Library page. Downloads can be queued at any granularity — a single episode, an entire season, a full series, or all episodes in a TV VOD year.
   - **Follow rules** — Create a follow rule in Download mode (instead of STRM mode) to automatically queue downloads for matching content as it appears in future ingestion runs.

2. **Processing** — The `Process Download Queue` scheduled task wakes on a configurable interval and claims up to `max_concurrent` pending rows. The processor also starts automatically whenever a new download is queued — no need to wait for the next scheduled run. Each row moves through a state machine:

   ```
   pending -> probing -> downloading -> completed
                                     -> failed (retry eligible)
                                     -> cancelled (terminal, with reason)
   ```

   - **Probing** — The stream URL is probed with `ffprobe` to verify it is reachable and to capture format metadata.
  - **Downloading** — Progressive media files such as MP4, MKV, and TS are downloaded byte-for-byte in their original container, preserving every video, audio, subtitle, data stream, chapter, and metadata field. Non-progressive inputs use ffmpeg stream copy with every input stream explicitly mapped. VODSTRM does not re-encode or silently drop incompatible streams; the download fails instead. The partial file uses a `.part` suffix beside its final offline destination.
  - **Finalizing** — The `.part` file is atomically renamed into the mirrored path under `VOD_OFFLINE_PATH` using the configured content folder (for example, `data/vod-offline/movies/Movie Title (Year)/Movie Title (Year).mp4` with defaults). VODSTRM then creates the matching hard link under `VOD_PATH`, and STRM sync removes the now-obsolete `.strm` file.

3. **STRM interaction** — When a download completes, the entry's `.strm` file is deleted and your media server picks up the hard-linked media file instead. If a download is cancelled or both file names disappear, the `.strm` file is regenerated on the next sync. During every STRM sync, completed downloads are reconciled across both mirrored trees; missing VOD links are repaired and filter-driven renames update both paths.

4. **Cancellation and failure handling** — The Cancel action stops an active HTTP transfer or ffmpeg process, removes its partial `.part` file, and retains the row as cancelled for retry. Failed downloads are retained with a reason (e.g. `probe_failed`, `ffmpeg_failed`, `hardlink_failed`, `no_eligible_stream`). They can be retried individually from the Downloads page or in bulk via the Clear Failed button. Failed rows are automatically cleaned up after 90 days; cancelled rows after 24 hours. Both happen during STRM sync.

5. **Provider independence** — Download rows are keyed by entry_id, not by stream or provider. If a provider is removed, the downloaded file and its database row survive. The foreign key uses ON DELETE SET NULL, so the row persists even if the underlying entry is orphaned.

6. **Automatic maintenance** — On startup, downloads stuck in probing or downloading are reset to pending. During STRM sync, orphaned `.part` and offline files are removed, existing VOD-only downloads are adopted into the offline tree, missing hard links are repaired, and completed downloads are cancelled only when neither path exists.

#### Configuration

On the Integrations page, the Downloads settings card provides:

- **Enable toggle** — Turn the integration on or off.
- **Max Concurrent** — Number of simultaneous downloads (1-10). Default is 2.
- **Container** — Output file format: `mkv` (default), `mp4`, or `ts`.
- **Retention (days)** — How long failed download rows are kept before automatic cleanup. Default is 90 days. Cancelled rows are cleaned after 24 hours.

The status widget shows queue counts by status (pending, probing, downloading, completed, failed, cancelled, total) and a **Process Now** button to trigger the processor immediately.

#### Downloads Page

**URL:** `/library/downloads`

The Downloads page provides a full view of the download queue:

- **Status pills** — Each row shows its current state with a colour-coded badge.
- **Actions** — Cancel active downloads, retry failed ones, delete completed files, or remove failed/cancelled rows.
- **Clear Failed** — Bulk-remove all failed and cancelled rows from the list (does not delete files).
- **Auto-refresh** — The list updates every 5 seconds.

The page is also linked from the Downloads section of the Integrations page.
