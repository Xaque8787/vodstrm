"""
Standalone migration runner.
Run directly from the IDE to apply all pending migrations without starting the full app.
Usage:
    python run_migrations.py
"""
import importlib.util
import logging
import os
import sqlite3
import sys

logger = logging.getLogger(__name__)

_DOCKER_ROOT = "/app"


def _project_root() -> str:
    if os.path.exists(_DOCKER_ROOT) and os.path.isfile(os.path.join(_DOCKER_ROOT, "run.py")):
        return _DOCKER_ROOT
    return os.path.dirname(os.path.abspath(__file__))


def _resolve(relative: str) -> str:
    if os.path.isabs(relative):
        return relative
    return os.path.join(_project_root(), relative)


MIGRATIONS_DIR = _resolve("migrations")


def _get_connection() -> sqlite3.Connection:
    try:
        from dotenv import load_dotenv
        load_dotenv(_resolve(".env"))
    except ImportError:
        pass

    db_path = _resolve(os.getenv("DATABASE_PATH", "data/app.db"))
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def _applied_migrations(conn: sqlite3.Connection) -> set:
    rows = conn.execute("SELECT filename FROM migrations").fetchall()
    return {row["filename"] for row in rows}


def _load_migration(path: str):
    spec = importlib.util.spec_from_file_location("migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_all_migrations() -> None:
    conn = _get_connection()
    _ensure_migrations_table(conn)
    applied = _applied_migrations(conn)

    migration_files = sorted(
        f for f in os.listdir(MIGRATIONS_DIR)
        if f.endswith(".py") and not f.startswith("_")
    )

    pending = [f for f in migration_files if f not in applied]

    if not pending:
        logger.info("No pending migrations.")
        return

    for filename in pending:
        path = os.path.join(MIGRATIONS_DIR, filename)
        logger.info("Applying migration: %s", filename)
        module = _load_migration(path)
        try:
            module.up(conn)
            conn.execute(
                "INSERT INTO migrations (filename) VALUES (?)", (filename,)
            )
            conn.commit()
            logger.info("Applied: %s", filename)
        except Exception as exc:
            conn.rollback()
            logger.error("Migration failed: %s — %s", filename, exc)
            raise

    conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    run_all_migrations()
