# VODSTRM

VODSTRM is a self-hosted media library manager that ingests M3U and Xtream Codes playlists, organizes the content into a structured `.strm` file library, and keeps everything in sync automatically. Point it at your IPTV providers, apply filters to clean up titles, and let your media server (Jellyfin, Plex, Emby, etc.) pick up the rest.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Quick Start](#quick-start)
  - [Configuration](#configuration)
    - [.env (Docker environment)](#env-docker-environment)
    - [app.env (Application settings)](#appenv-application-settings)
- [Usage](#usage)
  - [First Run & Setup](#first-run--setup)
  - [Providers](#providers)
  - [Library](#library)
  - [Filters](#filters)
  - [Schedules](#schedules)
  - [Admin](#admin)
- [Integrations](#integrations)
  - [TMDB](#tmdb)

---

## Features

- Supports M3U URL, Xtream Codes API, and local `.m3u` file providers
- Ingests and normalizes VOD content across multiple providers with priority-based deduplication
- Generates `.strm` files organized into Movies, Series, TV VOD, Live, and Unsorted categories
- Flexible filtering system — clean up messy titles, exclude unwanted content, or whitelist specific entries
- Automatic scheduling for downloads, ingestion, and library sync
- Follow rules to automatically add new episodes/seasons as they appear
- Per-provider STRM mode: generate everything or only import what you select
- Multi-user support with JWT-based authentication

---

## Installation

### Prerequisites

- Docker and Docker Compose

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
      - "${APP_PORT:-2112}:${APP_PORT:-2112}"
    # env_file:
    #   - app.env
    environment:
      - SECRET_KEY=${SECRET_KEY}
      - TZ=${TZ:-America/Los_Angeles}
      - PUID=${PUID:-1000}
      - PGID=${PGID:-1000}
    volumes:
      - ${DATA_PATH:-./data}:/app/data
      - ${VOD_PATH:-./data/vod}:/app/data/vod
      # Uncomment and point to a host path containing local .m3u files. Or place m3u files directly in /app/data/m3u.
      # - /path/on/host/to/m3u:/app/data/m3u
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:${APP_PORT:-2112}/login"]
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

VODSTRM uses two separate configuration files. Only `.env` is required to get started.

#### .env (Docker environment)

This file controls the Docker container itself — ports, paths, timezone, and secrets. Docker Compose reads it automatically when it exists in the same directory as `docker-compose.yml`.

```env
# A strong random string used to sign authentication tokens.
# Change this before deploying — any change will invalidate all active sessions.
SECRET_KEY=change-me-to-something-random

# Port the web UI is served on
APP_PORT=2112

# User and group ID the container process runs as.
# This controls file ownership for .strm output and data files.
# Run `id -u && id -g` on your host to find your values.
PUID=1000
PGID=1000

# Timezone used for the scheduler and displayed timestamps.
# Full list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
TZ=America/Los_Angeles

# Host path where app data is stored (SQLite databases, logs, downloaded M3U files).
DATA_PATH=./data

# Host path where .strm files are written.
# Point this at a directory your media server monitors (e.g. a Jellyfin library path or NAS mount).
# VOD_PATH=/mnt/nas/media
VOD_PATH=./data/vod
```

**Local M3U files:** If you have `.m3u` files on your host, you can either mount a directory into the container (uncomment the volume in `docker-compose.yml` and set the host path), or simply drop the files into `DATA_PATH/m3u` and add them as a Local File provider through the UI.

---

#### app.env (Application settings)

This file controls application-level behaviour. It is optional — the defaults shown below are used if the file is absent. To activate it, uncomment the `env_file` block in `docker-compose.yml`.

```env
# Path to the main SQLite database inside the container
DATABASE_PATH=data/app.db

# Path to the APScheduler jobs database inside the container
SCHEDULER_DB_PATH=data/scheduler.db

# How long a login session stays valid, in minutes
ACCESS_TOKEN_EXPIRE_MINUTES=60

# Set to true to enable debug logging and the interactive API docs at /docs
DEBUG=false

# Set to true when running behind HTTPS to mark auth cookies as Secure
SECURE_COOKIES=false
```

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

Enter your Xtream Codes server URL, username, and password. Optionally specify a port and choose a stream format (TS or HLS). VODSTRM constructs the API request automatically.

#### Local File

Select a `.m3u` file already present on the host (mounted into the container). Use the built-in file browser to navigate to the file rather than typing a path manually.

---

#### Provider Settings

Each provider has the following options available after creation:

- **Priority** — When multiple providers supply the same content, the provider with the lowest priority number wins. If two providers tie, they are broken alphabetically by their internal slug. This determines which URL ends up inside the `.strm` file.
- **Quality Terms** — An ordered list of plain-text terms used to score incoming streams before deciding whether they should overwrite an existing stream row for the same content. When quality terms are configured, each ingest run compares the incoming stream's raw title against the existing one: the stream whose title contains more matching terms wins. If the scores are equal the existing stream is kept. If no quality terms are configured the new stream always overwrites the existing one (the original behaviour).

  Terms are matched as whole words and are case-insensitive, so `hd` will not match `uhd` or `hdr`. The order of the list does not affect scoring — each term that appears in the title counts as one point regardless of position in the list.

  **Example:** With quality terms `["4k", "2160p", "1080p", "hd"]`, a stream titled `Movie Title 4K HDR` scores 2 (`4k` and `hd`) while one titled `Movie Title HD` scores 1. The 4K stream wins and its URL is written to the `.strm` file. On the next run, if the 4K version disappears from the provider's playlist entirely, the incoming lower-quality stream automatically wins because the existing row is from a previous run — stale rows never block a live incoming stream.
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

The Schedules page controls when VODSTRM automatically fetches and processes your providers.

#### Global Tasks

Two global tasks are available:

- **Download All Providers** — Downloads the latest M3U data from all active providers, runs ingestion, applies filters, and syncs `.strm` files. This is the primary task you will want to run on a regular schedule.
- **Clean STRM Orphans** — Scans the VOD output directory and removes any `.strm` files that no longer have a corresponding database entry. Useful for cleaning up after providers are deleted or content is removed.

Each global task can be:
- **Run Now** — Triggered immediately, outside of any schedule.
- **Scheduled** — Set a recurring trigger using either a cron expression (e.g. `0 3 * * *` for 3 AM daily) or a simple interval (e.g. every 6 hours).

#### Per-Provider Controls

The provider table below the global tasks shows each configured provider with the following controls:

- **Omit from Schedule** — Temporarily exclude a provider from all scheduled and global ingest runs without touching its data. When omitted, the provider's existing streams and entries remain in the database exactly as they are — they just will not be updated. An amber **Omitted** badge appears next to the provider name as a reminder that its data may be stale. Click the toggle again to include the provider in future runs. To manually run an omitted provider regardless, use the **Download Now** button. To disable a provider and remove its data, use the Disable action on the Providers page.
- **Download Now** — Manually trigger a download and ingest for a single provider without affecting others. Works even if the provider is omitted from the schedule.
- **STRM Mode** — Toggle between Generate All and Import Selected mode for the provider.

---

### Admin

The Admin section contains tools for inspecting the internal state of the application and managing users. It is intended for troubleshooting rather than day-to-day use.

#### Users

**URL:** `/admin/users`

Lists all user accounts. Any user can be deleted except the currently logged-in account. VODSTRM does not currently support self-service registration — new accounts must be created by an existing admin.

#### Library Inspector

**URL:** `/admin/library`

A low-level view into the entries and streams tables in the database.

- **Entries tab** — Shows every ingested media entry. Supports search, sorting, and pagination (100 per page). Useful for verifying that ingestion is working and titles are being parsed correctly.
- **Streams tab** — Shows the individual stream records attached to each entry, including which provider they came from, their current URL, and their `filtered_title`. Expand any row to inspect the raw metadata JSON and the filter hits that were applied during the last filter run.

The **Clear Entries** and **Clear Streams** buttons wipe the respective tables. Use with caution — clearing entries will remove all ownership and follow data, and the next ingestion run will treat everything as new.

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
