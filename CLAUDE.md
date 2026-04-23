# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the app

```bash
# Development (auto-reload)
python run.py

# Apply pending migrations only
python run_migrations.py

# Docker (production)
docker-compose up

# Docker (local dev with volume mounts)
docker-compose -f docker-compose.local.yml up
```

Copy `example.env` to `.env` before first run. The minimum required value is `SECRET_KEY`.

There are no tests or linting tools configured in this project.

## Startup sequence

On every startup `app/main.py` lifespan runs in order:
1. `init_db()` — applies `_SCHEMA` from `app/database.py` (idempotent `CREATE TABLE IF NOT EXISTS`)
2. `run_all_migrations()` — applies any pending files from `migrations/`
3. `start_scheduler()` — loads enabled rows from `task_schedules`, registers them with APScheduler

## Database rules — critical

**Every schema change requires updates in TWO places:**

1. **`app/database.py` `_SCHEMA`** — the canonical full schema. A fresh install only runs this; it must produce the complete, correct schema with no migrations needed. Always use `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`.

2. **`migrations/YYYY_MM_DD_<description>.py`** — alters existing installations. Each file exports `up(conn: sqlite3.Connection)`. For `ALTER TABLE ... ADD COLUMN`, SQLite has no `IF NOT EXISTS`, so check first:
   ```python
   existing = {row[1] for row in conn.execute("PRAGMA table_info(table_name)").fetchall()}
   if "column_name" not in existing:
       conn.execute("ALTER TABLE table_name ADD COLUMN column_name TYPE DEFAULT value")
   conn.commit()
   ```

The migration runner tracks applied files in the `migrations` table and skips already-applied ones. Never rely on migrations to create tables from scratch — that is `_SCHEMA`'s job.

## Architecture

### Data model

Two core tables form the library:

- **`entries`** — one row per unique piece of content, identified by a deterministic SHA256 `entry_id` hashed from `(type, cleaned_title, season, episode, year, air_date)`. Provider-agnostic; never stores provider-specific data.
- **`streams`** — one row per provider per entry (unique on `(entry_id, provider)`). Stores the stream URL, ingest metadata, and all filter outputs (`filtered_title`, `filter_hits`, `exclude`, `include_only`).

The same content from two providers → same `entry_id`, two `streams` rows.

### Ingestion pipeline

```
M3U file
  → app/ingestion/parser.py   parse_m3u()       — classify entries by regex
  → app/ingestion/sync.py     run_sync()         — upsert entries + streams
                                                 — clean stale streams (batch_id diff)
                                                 — clean orphaned entries
  → app/filters/engine.py     run_filters_for_provider()  — write filter outputs to streams
  → (delete M3U file, except local_file providers)
```

Content classification in the parser: `live` (duration=-1), `series` (S##E## pattern), `tv_vod` (air-date pattern), `movie` (year pattern), `unsorted` (fallback).

### Filter system

Filters are scoped per `(provider, entry_type)` — both support `'*'` wildcards. Four types execute in order: `replace` (literal find/replace on `cleaned_title`), `remove` (regex strip from title), `exclude` (regex match on `raw_title` → sets `exclude` flag), `include_only` (regex match → sets `include_only` flag). All outputs land on `streams`, never `entries`. Filters run automatically at the end of every ingest and can be manually reapplied via `POST /filters/reapply`.

### Task system

Tasks are plain decorated functions:
```python
@task("task_name")
def my_task(arg=None):
    ...
```

The `@task` decorator handles logging, timing, and error isolation. Schedules are rows in `task_schedules` (cron or interval triggers). `app/tasks/registry.py` reads that table at startup and registers jobs with APScheduler. Tasks can always be called directly as regular functions — the scheduler just calls them on a schedule.

### Adding a new task

1. Write the function in `app/tasks/` with `@task("name")`
2. Import and call it from `app/tasks/registry.py` so the scheduler can register it
3. Insert a row into `task_schedules` (or let the UI do it via `/schedules`)

### Routes and templates

All routes are auth-protected via `get_current_user` (JWT cookie). Each router lives in `app/routes/` and must be registered in `app/main.py` via `app.include_router()`. Templates use Jinja2 and extend `base.html`. Static assets are in `app/static/`.

### Path resolution

Use `app/utils/env.resolve_path()` for all file paths — it handles both Docker (`/app/`) and local dev environments transparently. Use `local_now_iso()` from the same module for all DB timestamps.

### Two SQLite databases

- `data/app.db` — application data (schema in `app/database.py`)
- `data/scheduler.db` — APScheduler job store (managed by APScheduler, not `_SCHEMA`)
