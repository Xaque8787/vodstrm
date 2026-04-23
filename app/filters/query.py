"""DB read/write helpers for filter rules."""
import sqlite3
from app.utils.env import local_now_iso


def list_filters(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, filter_type, label, order_index, enabled, created_at FROM filters ORDER BY filter_type, order_index"
    ).fetchall()
    result = []
    for row in rows:
        fid = row["id"]
        providers = [r["provider"] for r in conn.execute(
            "SELECT provider FROM filter_providers WHERE filter_id = ? ORDER BY provider", (fid,)
        ).fetchall()]
        entry_types = [r["entry_type"] for r in conn.execute(
            "SELECT entry_type FROM filter_entry_types WHERE filter_id = ? ORDER BY entry_type", (fid,)
        ).fetchall()]
        patterns = [
            {"id": r["id"], "pattern": r["pattern"], "replacement": r["replacement"], "order_index": r["order_index"]}
            for r in conn.execute(
                "SELECT id, pattern, replacement, order_index FROM filter_patterns WHERE filter_id = ? ORDER BY order_index",
                (fid,),
            ).fetchall()
        ]
        result.append({
            "id": fid,
            "filter_type": row["filter_type"],
            "label": row["label"],
            "order_index": row["order_index"],
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "providers": providers,
            "entry_types": entry_types,
            "patterns": patterns,
        })
    return result


def get_filter(conn: sqlite3.Connection, filter_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, filter_type, label, order_index, enabled FROM filters WHERE id = ?", (filter_id,)
    ).fetchone()
    if not row:
        return None
    providers = [r["provider"] for r in conn.execute(
        "SELECT provider FROM filter_providers WHERE filter_id = ?", (filter_id,)
    ).fetchall()]
    entry_types = [r["entry_type"] for r in conn.execute(
        "SELECT entry_type FROM filter_entry_types WHERE filter_id = ?", (filter_id,)
    ).fetchall()]
    patterns = [
        {"id": r["id"], "pattern": r["pattern"], "replacement": r["replacement"], "order_index": r["order_index"]}
        for r in conn.execute(
            "SELECT id, pattern, replacement, order_index FROM filter_patterns WHERE filter_id = ? ORDER BY order_index",
            (filter_id,),
        ).fetchall()
    ]
    return {
        "id": row["id"], "filter_type": row["filter_type"], "label": row["label"],
        "order_index": row["order_index"], "enabled": bool(row["enabled"]),
        "providers": providers, "entry_types": entry_types, "patterns": patterns,
    }


def create_filter(
    conn: sqlite3.Connection,
    filter_type: str, label: str, order_index: int,
    providers: list[str], entry_types: list[str], patterns: list[dict],
) -> int:
    now = local_now_iso()
    cursor = conn.execute(
        "INSERT INTO filters (filter_type, label, order_index, enabled, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?)",
        (filter_type, label, order_index, now, now),
    )
    fid = cursor.lastrowid
    _set_providers(conn, fid, providers)
    _set_entry_types(conn, fid, entry_types)
    _set_patterns(conn, fid, patterns)
    return fid


def update_filter(
    conn: sqlite3.Connection,
    filter_id: int, label: str, order_index: int,
    providers: list[str], entry_types: list[str], patterns: list[dict],
) -> None:
    now = local_now_iso()
    conn.execute(
        "UPDATE filters SET label=?, order_index=?, updated_at=? WHERE id=?",
        (label, order_index, now, filter_id),
    )
    _set_providers(conn, filter_id, providers)
    _set_entry_types(conn, filter_id, entry_types)
    _set_patterns(conn, filter_id, patterns)


def toggle_filter(conn: sqlite3.Connection, filter_id: int) -> None:
    now = local_now_iso()
    conn.execute(
        "UPDATE filters SET enabled = CASE WHEN enabled=1 THEN 0 ELSE 1 END, updated_at=? WHERE id=?",
        (now, filter_id),
    )


def delete_filter(conn: sqlite3.Connection, filter_id: int) -> None:
    conn.execute("DELETE FROM filters WHERE id=?", (filter_id,))


def list_provider_slugs(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT slug FROM providers ORDER BY name").fetchall()
    return [r["slug"] for r in rows]


def _set_providers(conn: sqlite3.Connection, filter_id: int, providers: list[str]) -> None:
    conn.execute("DELETE FROM filter_providers WHERE filter_id=?", (filter_id,))
    for p in providers:
        conn.execute("INSERT INTO filter_providers (filter_id, provider) VALUES (?, ?)", (filter_id, p))


def _set_entry_types(conn: sqlite3.Connection, filter_id: int, entry_types: list[str]) -> None:
    conn.execute("DELETE FROM filter_entry_types WHERE filter_id=?", (filter_id,))
    for et in entry_types:
        conn.execute("INSERT INTO filter_entry_types (filter_id, entry_type) VALUES (?, ?)", (filter_id, et))


def _set_patterns(conn: sqlite3.Connection, filter_id: int, patterns: list[dict]) -> None:
    conn.execute("DELETE FROM filter_patterns WHERE filter_id=?", (filter_id,))
    for idx, p in enumerate(patterns):
        conn.execute(
            "INSERT INTO filter_patterns (filter_id, pattern, replacement, order_index) VALUES (?, ?, ?, ?)",
            (filter_id, p["pattern"], p.get("replacement"), idx),
        )
