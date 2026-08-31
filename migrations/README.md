# Migrations

Migrations are for **schema changes to existing tables** — adding columns, removing columns,
renaming columns, adding indexes, etc.

**New tables are NOT created via migrations.** All `CREATE TABLE IF NOT EXISTS` statements
live in `app/database.py` inside `init_db()`, which runs automatically at startup.

---

## When to add a migration

- Adding a column to an existing table
- Removing a column from an existing table
- Renaming a column
- Adding or dropping an index
- Backfilling or transforming existing data

## When NOT to add a migration

- Creating a brand new table — add it to `_SCHEMA` in `app/database.py` instead

---

## Naming Convention

Use date-prefixed filenames to control execution order:

```
YYYY_MM_DD_<description>.py
```

Example:
```
2026_05_01_add_active_flag_to_providers.py
```

---

## Migration File Structure

Each migration must define an `up(conn, logger=None)` function. The `logger` parameter
is optional — the migration runner passes its logger so migrations can report what they
checked and what they changed. If called without a logger, the migration falls back to
its own logger via `logging.getLogger(__name__)`.

A `down(conn)` is optional and not currently used.

```python
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "is_active" in existing:
        log.info("  providers.is_active already exists, skipping")
        return
    log.info("  Adding providers.is_active column")
    conn.execute("ALTER TABLE providers ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    conn.commit()
```

---

## The Idempotency Rule (Critical)

**Every migration must be idempotent.** On a fresh install, `init_db()` runs first and
creates the full schema via `_SCHEMA` (using `CREATE TABLE IF NOT EXISTS`). Then the
migration runner executes every migration that is not yet recorded in the `migrations`
table. On a fresh install, that means every migration runs — and each one must detect
that its changes are already present and skip the actual work.

This means each migration must **check the database state before modifying anything**,
and **log what it found and what it did** (or didn't do).

### How to check for each operation type

| Operation | Check method | Safe SQL |
|-----------|--------------|----------|
| `ADD COLUMN` | `PRAGMA table_info(table)` → check column name in set | `ALTER TABLE ... ADD COLUMN` (only if missing) |
| `CREATE TABLE` | `SELECT name FROM sqlite_master WHERE type='table'` | `CREATE TABLE IF NOT EXISTS` |
| `CREATE INDEX` | `PRAGMA index_list(table)` → check index name in set | `CREATE INDEX IF NOT EXISTS` |
| `DROP INDEX` | `PRAGMA index_list(table)` → check index name in set | `DROP INDEX IF EXISTS` |
| `DROP TABLE` | `SELECT name FROM sqlite_master WHERE type='table'` | `DROP TABLE IF EXISTS` |
| Data backfill | `SELECT COUNT(*) ... WHERE condition` | `UPDATE ...` only if count > 0 |

### SQLite limitation: no `IF NOT EXISTS` on `ADD COLUMN`

SQLite does not support `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. You must check
`PRAGMA table_info(table_name)` first:

```python
existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
if "my_column" not in existing:
    conn.execute("ALTER TABLE providers ADD COLUMN my_column TEXT")
```

### Recreating a table to change a CHECK constraint

SQLite cannot alter a CHECK constraint in-place. Use the rename-create-insert-drop
pattern. On a fresh install where `_SCHEMA` already has the updated constraint, detect
this and skip the rebuild:

```python
schema = conn.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name='follows'"
).fetchone()
if schema and "tv_vod" in (schema["sql"] or ""):
    log.info("  follows.entry_type already includes tv_vod, skipping rebuild")
    return
```

---

## Logging Conventions

Every migration should log at the `INFO` level using the passed-in logger. Indent
log messages with two spaces so they nest under the runner's "Applying migration:"
line. Log messages should describe:

- What was checked ("providers.is_active already exists, skipping")
- What was done ("Adding providers.is_active column")
- How many rows were affected when relevant ("Backfilled 3 slug values")

### Expected log output

**Fresh install** (schema already created by `_SCHEMA`):
```
INFO | Applying migration: 2026_04_19_add_provider_is_active.py
INFO |   providers.is_active already exists, skipping
INFO | Applied: 2026_04_19_add_provider_is_active.py
```

**Upgrade** (existing database, migration not yet applied):
```
INFO | Applying migration: 2026_04_19_add_provider_is_active.py
INFO |   Adding providers.is_active column
INFO | Applied: 2026_04_19_add_provider_is_active.py
```

**Upgrade** (migration already applied in a previous run):
```
INFO | No pending migrations.
```

---

## Running Migrations

**On app startup** — applied automatically via the lifespan hook in `app/main.py`.

**From the IDE** — run `run_migrations.py` directly:
```
python run_migrations.py
```

---

## How It Works

1. `init_db()` in `app/database.py` ensures all tables exist (idempotent `CREATE TABLE IF NOT EXISTS`).
2. A `migrations` table tracks which migration files have already been applied.
3. On each startup, only pending files (not in the `migrations` table) are executed, in ascending filename order.
4. Each migration's `up(conn, logger=logger)` is called. If the migration does not accept
   a `logger` parameter, it is called as `up(conn)` instead (backwards compatible).
5. After a migration completes, its filename is recorded in the `migrations` table so it
   is not run again on future startups.

### Fresh install vs upgrade

- **Fresh install:** `_SCHEMA` creates the complete schema. Every migration runs but
  each one detects its changes are already present and logs "already exists, skipping."
  This is expected and correct — the migrations are recorded as applied so they don't
  run again.

- **Upgrade (new image on existing database):** Previously-applied migrations are skipped
  (they're in the `migrations` table). New migrations run, detect the database is in the
  old state, apply their changes, and log what they did.
