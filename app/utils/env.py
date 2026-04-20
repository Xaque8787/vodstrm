import os
from datetime import datetime, timezone

_DOCKER_ROOT = "/app"


def is_docker() -> bool:
    return os.path.exists(_DOCKER_ROOT) and os.path.isfile(os.path.join(_DOCKER_ROOT, "run.py"))


def project_root() -> str:
    if is_docker():
        return _DOCKER_ROOT
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def resolve_path(*parts: str) -> str:
    raw = os.path.join(*parts)
    if os.path.isabs(raw):
        return raw
    return os.path.join(project_root(), raw)


def local_now() -> datetime:
    """
    Return the current time as a timezone-aware datetime in the configured
    local timezone (TZ env var).  Falls back to UTC if the zone is unknown.
    """
    try:
        import zoneinfo
        tz_name = os.getenv("TZ", "UTC")
        tz = zoneinfo.ZoneInfo(tz_name)
        return datetime.now(tz)
    except Exception:
        return datetime.now(timezone.utc)


def local_now_iso() -> str:
    """Return local_now() as an ISO 8601 string (suitable for DB storage)."""
    return local_now().isoformat()
