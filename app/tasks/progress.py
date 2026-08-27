"""Thread-safe runtime progress state for background provider synchronization."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

_states: dict[str, dict[str, Any]] = {}
_guard = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def start(provider_slug: str, message: str = "Starting provider sync") -> None:
    now = _now()
    with _guard:
        _states[provider_slug] = {
            "provider_slug": provider_slug,
            "status": "running",
            "phase": "starting",
            "message": message,
            "current": 0,
            "total": None,
            "started_at": now,
            "updated_at": now,
            "completed_at": None,
        }


def update(
    provider_slug: str,
    *,
    phase: str,
    message: str,
    current: int | None = None,
    total: int | None = None,
    **details: Any,
) -> None:
    now = _now()
    with _guard:
        state = _states.setdefault(
            provider_slug,
            {
                "provider_slug": provider_slug,
                "status": "running",
                "started_at": now,
                "completed_at": None,
            },
        )
        state.update(
            {
                "status": "running",
                "phase": phase,
                "message": message,
                "current": current,
                "total": total,
                "updated_at": now,
                **details,
            }
        )


def finish(provider_slug: str, message: str = "Provider sync complete") -> None:
    now = _now()
    with _guard:
        state = _states.setdefault(provider_slug, {"provider_slug": provider_slug})
        state.update(
            {
                "status": "completed",
                "phase": "completed",
                "message": message,
                "current": state.get("total"),
                "updated_at": now,
                "completed_at": now,
            }
        )


def fail(provider_slug: str, message: str = "Provider sync failed") -> None:
    now = _now()
    with _guard:
        state = _states.setdefault(provider_slug, {"provider_slug": provider_slug})
        state.update(
            {
                "status": "failed",
                "phase": "failed",
                "message": message,
                "updated_at": now,
                "completed_at": now,
            }
        )


def snapshot(provider_slug: str | None = None) -> dict[str, Any]:
    with _guard:
        if provider_slug is not None:
            return dict(_states.get(provider_slug, {}))
        return {slug: dict(state) for slug, state in _states.items()}


def clear() -> None:
    """Clear runtime states. Intended for tests."""
    with _guard:
        _states.clear()
