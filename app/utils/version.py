import logging

import requests

from app.utils.env import resolve_path

logger = logging.getLogger(__name__)

REMOTE_VERSION_URL = "https://raw.githubusercontent.com/Xaque8787/vodstrm/refs/heads/master/.dockerversion"


def get_local_version() -> str:
    try:
        with open(resolve_path(".dockerversion"), "r") as f:
            return f.read().strip()
    except (OSError, IOError):
        return "unknown"


def get_remote_version() -> str | None:
    try:
        resp = requests.get(REMOTE_VERSION_URL, timeout=5)
        if resp.status_code == 200:
            return resp.text.strip()
        return None
    except requests.RequestException:
        return None


def check_version() -> tuple[str, bool]:
    local = get_local_version()
    remote = get_remote_version()
    update_available = remote is not None and remote != local
    return local, update_available
