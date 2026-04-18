# Migrations

This directory contains database migration scripts applied in filename order.

## Naming Convention

Use date-prefixed filenames to control execution order:

```
YYYY_MM_DD_<description>.py
```

Example:
```
2026_04_18_create_users.py
```

## Migration File Structure

Each migration file must define an `up(conn)` function that receives an open
`sqlite3.Connection`. A `down(conn)` function is optional but recommended.

```python
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS example (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)


def down(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS example")
```

## Running Migrations

**On app startup** — migrations are applied automatically via the lifespan hook
in `app/main.py`, which calls `run_migrations.run_all_migrations()`.

**From the IDE** — run `run_migrations.py` directly:
```
python run_migrations.py
```

**Individual migration** — each file can also be run directly for development:
```
python migrations/2026_04_18_create_users.py
```

## How It Works

1. A `migrations` table is created in the database on first run.
2. Applied migration filenames are recorded in that table.
3. On each run, only files not yet in the table are executed.
4. Migrations are applied in ascending filename order.
