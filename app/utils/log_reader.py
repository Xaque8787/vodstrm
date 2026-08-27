"""Safe, bounded access to current and rotated application logs."""
import os
import re
from datetime import datetime

from app.config import settings
from app.utils.env import resolve_path


LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_LOG_DIR = resolve_path(settings.log_dir)
_LOG_NAME_RE = re.compile(r"^app\.log(?:\.([1-9]\d*))?$")
_LOG_LINE_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    r" \| (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s*"
    r" \| (?P<logger>[^|]+?) \| (?P<message>.*)$"
)


def available_log_files() -> list[dict]:
    if not os.path.isdir(_LOG_DIR):
        return []
    files = []
    for name in os.listdir(_LOG_DIR):
        match = _LOG_NAME_RE.fullmatch(name)
        if not match:
            continue
        path = os.path.join(_LOG_DIR, name)
        if os.path.islink(path) or not os.path.isfile(path):
            continue
        stat = os.stat(path)
        rotation = int(match.group(1) or 0)
        files.append(
            {
                "name": name,
                "size": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "rotation": rotation,
            }
        )
    return sorted(files, key=lambda value: value["rotation"])


def _log_path(name: str) -> str:
    if not _LOG_NAME_RE.fullmatch(name):
        raise ValueError("Unknown log file")
    path = os.path.abspath(os.path.join(_LOG_DIR, name))
    if (
        os.path.dirname(path) != os.path.abspath(_LOG_DIR)
        or os.path.islink(path)
        or not os.path.isfile(path)
        or os.path.dirname(os.path.realpath(path)) != os.path.realpath(_LOG_DIR)
    ):
        raise ValueError("Unknown log file")
    return path


def _tail_lines(path: str, count: int) -> list[str]:
    chunks: list[bytes] = []
    newline_count = 0
    with open(path, "rb") as source:
        source.seek(0, os.SEEK_END)
        position = source.tell()
        while position > 0 and newline_count <= count:
            read_size = min(64 * 1024, position)
            position -= read_size
            source.seek(position)
            chunk = source.read(read_size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")
    text = b"".join(reversed(chunks)).decode("utf-8", errors="replace")
    return text.splitlines()[-count:]


def _parse_entries(lines: list[str]) -> list[dict]:
    entries: list[dict] = []
    for line in lines:
        match = _LOG_LINE_RE.match(line)
        if match:
            value = match.groupdict()
            value["logger"] = value["logger"].strip()
            entries.append(value)
        elif entries:
            entries[-1]["message"] += f"\n{line}"
    return entries


def read_log_entries(
    name: str = "app.log",
    level: str = "ALL",
    search: str = "",
    limit: int = 250,
) -> list[dict]:
    normalized_level = level.upper().strip()
    if normalized_level != "ALL" and normalized_level not in LOG_LEVELS:
        raise ValueError("Unknown log level")
    limit = max(50, min(int(limit), 2000))
    scan_lines = min(max(limit * 20, 5000), 40000)
    entries = _parse_entries(_tail_lines(_log_path(name), scan_lines))

    needle = search.strip().casefold()
    filtered = [
        entry
        for entry in entries
        if (normalized_level == "ALL" or entry["level"] == normalized_level)
        and (
            not needle
            or needle in entry["logger"].casefold()
            or needle in entry["message"].casefold()
        )
    ]
    return list(reversed(filtered[-limit:]))