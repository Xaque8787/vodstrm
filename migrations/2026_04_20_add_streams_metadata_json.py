"""
Migration: add metadata_json column to streams table

Adds a nullable TEXT column that stores a JSON object containing all raw
EXTINF key-value attributes (tvg-id, tvg-name, tvg-logo, group-title, any
provider-specific fields) plus the EXTGRP value when present.

This column is populated on every ingest run — old rows will remain NULL
until the provider is re-ingested, at which point the field is filled in.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(streams)").fetchall()}
    if "metadata_json" in existing:
        log.info("  streams.metadata_json already exists, skipping")
        return
    log.info("  Adding streams.metadata_json column")
    conn.execute("ALTER TABLE streams ADD COLUMN metadata_json TEXT")
    conn.commit()
