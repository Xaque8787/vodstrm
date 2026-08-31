"""Ensure the downloads integration is enabled by default for existing installs.

Older installs may have saved settings with enabled=false (the previous
default). This migration sets enabled=true if the key is missing or explicitly
false, so that auto-processing works without a manual visit to the
Integrations page.
"""
import json
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    row = conn.execute(
        "SELECT settings FROM integrations WHERE slug = 'downloads'"
    ).fetchone()
    if not row:
        log.info("  Inserting downloads integration row with enabled=true")
        conn.execute(
            "INSERT INTO integrations (slug, settings, updated_at) "
            "VALUES ('downloads', ?, datetime('now'))",
            (json.dumps({"enabled": True}),),
        )
    else:
        try:
            saved = json.loads(row["settings"] or "{}")
        except (ValueError, TypeError):
            saved = {}
        if not saved.get("enabled"):
            log.info("  Enabling downloads integration (was disabled or missing)")
            saved["enabled"] = True
            conn.execute(
                "UPDATE integrations SET settings=?, updated_at=datetime('now') "
                "WHERE slug='downloads'",
                (json.dumps(saved),),
            )
        else:
            log.info("  Downloads integration already enabled, skipping")
    conn.commit()
