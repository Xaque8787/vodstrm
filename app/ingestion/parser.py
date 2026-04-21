"""
M3U Parsing Layer — pure ingestion and classification.

Reads raw .m3u files and converts each playlist entry into a structured
Python dict. This layer does NOT filter, deduplicate, or persist anything.
Every entry present in the source file is captured exactly once.

Output dict keys per entry:
    raw_title, cleaned_title, type, attributes (all EXTINF key=value pairs),
    group-title / extgrp, season, episode, air_date, year, series_type,
    stream_url, source_file, provider, ingested_at, batch_id, entry_id
"""
import hashlib
import json
import logging
import re
from collections import defaultdict
from typing import Any

from app.utils.env import local_now_iso

logger = logging.getLogger("app.ingestion.parser")

# ---------------------------------------------------------------------------
# REGEX
# ---------------------------------------------------------------------------

_SEASON_EPISODE_RE = re.compile(
    r"\b(?:S(?P<season>\d{1,3})[ ._-]?E(?P<episode>\d{1,3})"
    r"|(?P<season2>\d{1,3})[xX](?P<episode2>\d{1,3}))\b",
    re.IGNORECASE,
)

_YEAR_RE = re.compile(r"\b(?:19\d{2}|20\d{2})\b")

_AIR_DATE_RE = re.compile(
    r"\b(?:"
    r"(?:19|20)\d{2}[ ._-]\d{2}[ ._-]\d{2}"
    r"|"
    r"\d{2}[ ._-]\d{2}[ ._-](?:19|20)\d{2}"
    r")\b"
)

_ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')


# ---------------------------------------------------------------------------
# INGESTION LOGGER
# ---------------------------------------------------------------------------

class _IngestionLogger:
    def __init__(self) -> None:
        self.errors: dict[str, list[dict]] = defaultdict(list)
        self.stats: dict[str, int] = {
            "total_lines": 0,
            "entries_started": 0,
            "entries_completed": 0,
            "movie": 0,
            "series": 0,
            "live": 0,
            "tv_vod": 0,
            "unsorted": 0,
            "errors": 0,
        }

    def log_error(self, category: str, line_number: int, line: str, error: Exception) -> None:
        self.errors[category].append(
            {"line_number": line_number, "line": line, "error": str(error)}
        )
        self.stats["errors"] += 1
        logger.debug(
            "[PARSER] Parse error (%s) at line %d: %s — %s",
            category, line_number, line[:120], error,
        )

    def increment(self, key: str) -> None:
        if key in self.stats:
            self.stats[key] += 1

    def summary(self) -> dict:
        return {"stats": dict(self.stats), "error_buckets": dict(self.errors)}


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _clean_name(value: str) -> str:
    return re.sub(r"[._\- ]+$", "", value).strip()


def _make_batch_id(file_path: str, provider: str, ingest_time: str) -> str:
    raw = f"{file_path}:{provider}:{ingest_time}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _make_entry_id(entry: dict) -> str:
    """
    Deterministic hash of content identity.
    Same content from different providers → same entry_id.
    """
    parts = ":".join([
        entry.get("type", ""),
        (entry.get("cleaned_title") or "").lower(),
        str(entry.get("season") or ""),
        str(entry.get("episode") or ""),
        str(entry.get("year") or ""),
        str(entry.get("air_date") or ""),
    ])
    return hashlib.sha256(parts.encode()).hexdigest()


# Keys that are internal bookkeeping — excluded from the provider metadata blob.
_INTERNAL_KEYS = frozenset({
    "stream_url", "source_file", "provider", "ingested_at", "batch_id",
    "entry_id", "type", "raw_title", "cleaned_title", "season", "episode",
    "year", "air_date", "series_type",
})


def _build_metadata_json(entry: dict) -> str:
    """
    Collect all raw EXTINF attributes plus extgrp into a JSON string.
    Internal bookkeeping keys are excluded so only provider-supplied
    metadata ends up in the blob.
    """
    meta: dict[str, Any] = {
        k: v for k, v in entry.items() if k not in _INTERNAL_KEYS
    }
    return json.dumps(meta, ensure_ascii=False)


def _extract_air_date(value: str) -> tuple[str | None, str]:
    match = _AIR_DATE_RE.search(value)
    if not match:
        return None, value
    raw = match.group(0)
    # normalise separator to '-'
    formatted = re.sub(r"[ ._]", "-", raw)
    cleaned = re.sub(re.escape(raw), "", value).strip()
    return formatted, cleaned


# ---------------------------------------------------------------------------
# EXTINF PARSER
# ---------------------------------------------------------------------------

def _parse_extinf(line: str) -> dict[str, Any]:
    content = line[len("#EXTINF:"):]
    duration_part, rest = content.split(" ", 1)

    result: dict[str, Any] = {"duration": duration_part.strip()}

    last_quote = rest.rfind('"')
    if last_quote != -1:
        attr_string = rest[: last_quote + 1]
        name = rest[last_quote + 2 :].strip()
    else:
        attr_string = ""
        name = rest.strip()

    result["name"] = name

    for key, value in _ATTR_RE.findall(attr_string):
        result[key] = value.strip()

    return result


# ---------------------------------------------------------------------------
# CLASSIFICATION
# ---------------------------------------------------------------------------

def _classify(entry: dict) -> dict:
    name = entry.get("name", "").strip()
    entry["raw_title"] = name

    # LIVE — duration == -1
    if entry.get("duration") == "-1":
        entry["type"] = "live"
        entry["cleaned_title"] = _clean_name(name)
        logger.debug("[PARSER] Classified as LIVE: %s", name[:80])
        return entry

    # SERIES — S##E## or ##x## pattern
    match = _SEASON_EPISODE_RE.search(name)
    if match:
        season = match.group("season") or match.group("season2")
        episode = match.group("episode") or match.group("episode2")
        entry["type"] = "series"
        entry["series_type"] = "season_episode"
        entry["season"] = int(season)
        entry["episode"] = int(episode)
        entry["cleaned_title"] = _clean_name(name[: match.start()])
        logger.debug(
            "[PARSER] Classified as SERIES S%sE%s: %s",
            season, episode, entry["cleaned_title"][:80],
        )
        return entry

    # TV VOD — air-date pattern
    air_date, cleaned = _extract_air_date(name)
    if air_date:
        entry["type"] = "tv_vod"
        entry["series_type"] = "air_date"
        entry["air_date"] = air_date
        entry["cleaned_title"] = _clean_name(cleaned)
        logger.debug("[PARSER] Classified as TV_VOD (%s): %s", air_date, entry["cleaned_title"][:80])
        return entry

    # MOVIE — year present
    year_matches = list(_YEAR_RE.finditer(name))
    if year_matches:
        last = year_matches[-1]
        entry["type"] = "movie"
        entry["year"] = int(last.group(0))
        entry["cleaned_title"] = _clean_name(name[: last.start()])
        logger.debug(
            "[PARSER] Classified as MOVIE (%d): %s",
            entry["year"], entry["cleaned_title"][:80],
        )
        return entry

    # UNSORTED
    entry["type"] = "unsorted"
    entry["cleaned_title"] = _clean_name(name)
    logger.debug("[PARSER] Classified as UNSORTED: %s", name[:80])
    return entry


# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------

def parse_m3u(file_path: str, provider: str, ingest_time: str | None = None) -> dict:
    """
    Parse an M3U file and return structured entry lists plus a summary.

    Returns:
        {
            "movies":   [...],
            "series":   [...],
            "live_tv":  [...],
            "tv_vod":   [...],
            "unsorted": [...],
            "batch_id": str,
            "summary":  { stats, error_buckets }
        }
    """
    if ingest_time is None:
        ingest_time = local_now_iso()

    batch_id = _make_batch_id(file_path, provider, ingest_time)
    log = _IngestionLogger()

    logger.info(
        "[PARSER] Starting parse — file=%s  provider=%s  batch=%s",
        file_path, provider, batch_id,
    )

    movies: list[dict] = []
    series: list[dict] = []
    live_tv: list[dict] = []
    tv_vod: list[dict] = []
    unsorted: list[dict] = []

    current: dict | None = None

    with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
        for line_number, raw_line in enumerate(fh, start=1):
            log.increment("total_lines")
            line = raw_line.strip()

            if not line:
                continue

            try:
                if line.startswith("#EXTINF"):
                    current = _parse_extinf(line)
                    current["source_file"] = file_path
                    current["provider"] = provider
                    current["ingested_at"] = ingest_time
                    current["batch_id"] = batch_id
                    log.increment("entries_started")
                    continue

                if line.startswith("#EXTGRP") and current is not None:
                    current["extgrp"] = line.split(":", 1)[1].strip()
                    continue

                # Skip other directives / comment lines
                if line.startswith("#"):
                    continue

                if current is not None and not line.startswith("#"):
                    current["stream_url"] = line
                    final = _classify(current.copy())
                    final["entry_id"] = _make_entry_id(final)
                    final["metadata_json"] = _build_metadata_json(final)
                    log.increment("entries_completed")

                    t = final["type"]
                    log.increment(t)

                    if t == "movie":
                        movies.append(final)
                    elif t == "series":
                        series.append(final)
                    elif t == "live":
                        live_tv.append(final)
                    elif t == "tv_vod":
                        tv_vod.append(final)
                    else:
                        unsorted.append(final)

                    current = None

            except Exception as exc:
                log.log_error("parse_failure", line_number, line, exc)
                current = None

    summary = log.summary()
    logger.info(
        "[PARSER] Completed — entries=%d  movie=%d  series=%d  live=%d  tv_vod=%d  unsorted=%d  errors=%d",
        summary["stats"]["entries_completed"],
        summary["stats"]["movie"],
        summary["stats"]["series"],
        summary["stats"]["live"],
        summary["stats"]["tv_vod"],
        summary["stats"]["unsorted"],
        summary["stats"]["errors"],
    )

    return {
        "movies": movies,
        "series": series,
        "live_tv": live_tv,
        "tv_vod": tv_vod,
        "unsorted": unsorted,
        "batch_id": batch_id,
        "summary": summary,
    }
