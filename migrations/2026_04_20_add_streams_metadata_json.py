"""
Migration: add metadata_json column to streams table

Adds a nullable TEXT column that stores a JSON object containing all raw
EXTINF key-value attributes (tvg-id, tvg-name, tvg-logo, group-title, any
provider-specific fields) plus the EXTGRP value when present.

This column is populated on every ingest run — old rows will remain NULL
until the provider is re-ingested, at which point the field is filled in.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.execute(
        "ALTER TABLE streams ADD COLUMN metadata_json TEXT"
    )
    conn.commit()
