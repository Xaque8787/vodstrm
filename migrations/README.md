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

Each migration must define an `up(conn)` function. A `down(conn)` is optional.

```python
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE providers ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")


def down(conn: sqlite3.Connection) -> None:
    pass
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

1. `init_db()` in `app/database.py` ensures all tables exist (idempotent).
2. A `migrations` table tracks which migration files have already been applied.
3. On each startup, only pending files are executed, in ascending filename order.
