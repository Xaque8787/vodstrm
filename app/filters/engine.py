"""
Filter execution engine.

All filter outputs are written to the streams table — never to entries.
Pipeline per stream (always starting from entry.cleaned_title):
  1. Replace Terms  — user literal → safe regex replace, ordered by order_index
  2. Remove Terms   — regex patterns stripped from title, ordered by order_index
  3. Normalise whitespace, fallback to cleaned_title if result is empty
  4. Exclude Terms  — regex match against raw_title → streams.exclude flag
  5. Include Only   — regex match against raw_title → streams.include_only flag
  6. filter_hits    — accumulated for Remove / Exclude / Include matches only
"""
import json
import logging
import re
import sqlite3
from typing import Any

logger = logging.getLogger("app.filters.engine")


def _matches_scope(
    filter_providers: list[str],
    filter_types: list[str],
    provider: str,
    entry_type: str,
) -> bool:
    provider_ok = ("*" in filter_providers) or (provider in filter_providers)
    type_ok = ("*" in filter_types) or (entry_type in filter_types)
    return provider_ok and type_ok


def _compile(pattern: str) -> re.Pattern | None:
    try:
        return re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        logger.warning("[FILTERS] Bad regex %r: %s", pattern, exc)
        return None


def load_filters(conn: sqlite3.Connection) -> list[dict]:
    """Return all enabled filter rules with scope and patterns, ordered for execution."""
    rows = conn.execute(
        "SELECT id, filter_type, order_index FROM filters WHERE enabled = 1 ORDER BY filter_type, order_index"
    ).fetchall()

    result = []
    for row in rows:
        fid = row["id"]
        providers = [r["provider"] for r in conn.execute(
            "SELECT provider FROM filter_providers WHERE filter_id = ?", (fid,)
        ).fetchall()]
        entry_types = [r["entry_type"] for r in conn.execute(
            "SELECT entry_type FROM filter_entry_types WHERE filter_id = ?", (fid,)
        ).fetchall()]
        patterns = [dict(r) for r in conn.execute(
            "SELECT pattern, replacement, order_index FROM filter_patterns WHERE filter_id = ? ORDER BY order_index",
            (fid,),
        ).fetchall()]
        result.append({
            "id": fid,
            "filter_type": row["filter_type"],
            "order_index": row["order_index"],
            "providers": providers,
            "entry_types": entry_types,
            "patterns": patterns,
        })
    return result


def apply_filters(stream: dict[str, Any], entry: dict[str, Any], filters: list[dict]) -> dict[str, Any]:
    """Apply all filter rules to one stream+entry pair. Returns dict of filter output columns."""
    provider = stream.get("provider", "")
    entry_type = entry.get("type", "")
    cleaned_title = entry.get("cleaned_title") or ""
    raw_title = entry.get("raw_title") or ""

    replace_rules, remove_rules, exclude_rules, include_only_rules = [], [], [], []
    for f in filters:
        if not _matches_scope(f["providers"], f["entry_types"], provider, entry_type):
            continue
        ft = f["filter_type"]
        if ft == "replace":
            replace_rules.append(f)
        elif ft == "remove":
            remove_rules.append(f)
        elif ft == "exclude":
            exclude_rules.append(f)
        elif ft == "include_only":
            include_only_rules.append(f)

    hits: list[str] = []
    working = cleaned_title

    # Step 1: Replace (user literal, safe regex)
    for rule in replace_rules:
        for pat in rule["patterns"]:
            compiled = _compile(re.escape(pat["pattern"]))
            if compiled:
                working = compiled.sub(pat["replacement"] or "", working)

    # Step 2: Remove (raw regex patterns)
    for rule in remove_rules:
        for pat in rule["patterns"]:
            compiled = _compile(pat["pattern"])
            if compiled:
                matches = compiled.findall(working)
                if matches:
                    hits.extend(str(m) for m in matches)
                    working = compiled.sub("", working)

    # Step 3: Normalise
    cleaned_result = " ".join(working.split()).strip()
    filtered_title = cleaned_result if cleaned_result else cleaned_title

    # Step 4: Exclude
    exclude = 0
    for rule in exclude_rules:
        for pat in rule["patterns"]:
            compiled = _compile(pat["pattern"])
            if compiled and compiled.search(raw_title):
                hits.append(pat["pattern"])
                exclude = 1
                break
        if exclude:
            break

    # Step 5: Include Only
    include_only = 0
    if include_only_rules:
        for rule in include_only_rules:
            for pat in rule["patterns"]:
                compiled = _compile(pat["pattern"])
                if compiled and compiled.search(raw_title):
                    hits.append(pat["pattern"])
                    include_only = 1
                    break
            if include_only:
                break

    return {
        "filtered_title": filtered_title,
        "filter_hits": json.dumps(hits),
        "exclude": exclude,
        "include_only": include_only,
    }


def run_filters_for_provider(
    conn: sqlite3.Connection,
    filters: list[dict],
    provider: str | None = None,
) -> int:
    """Apply filters to all streams for a provider (or all streams if provider=None). Returns count updated."""
    if provider:
        rows = conn.execute(
            """SELECT s.stream_id, s.provider, e.cleaned_title, e.raw_title, e.type
               FROM streams s JOIN entries e ON s.entry_id = e.entry_id WHERE s.provider = ?""",
            (provider,),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT s.stream_id, s.provider, e.cleaned_title, e.raw_title, e.type
               FROM streams s JOIN entries e ON s.entry_id = e.entry_id"""
        ).fetchall()

    updated = 0
    for row in rows:
        result = apply_filters(
            {"provider": row["provider"]},
            {"cleaned_title": row["cleaned_title"], "raw_title": row["raw_title"], "type": row["type"]},
            filters,
        )
        conn.execute(
            """UPDATE streams SET filtered_title=?, filter_hits=?, exclude=?, include_only=? WHERE stream_id=?""",
            (result["filtered_title"], result["filter_hits"], result["exclude"], result["include_only"], row["stream_id"]),
        )
        updated += 1

    logger.info("[FILTERS] Run complete — provider=%s  updated=%d", provider or "*", updated)
    return updated
