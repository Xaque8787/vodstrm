"""Central application configuration loaded from the project-root .env file."""
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "example.env", override=False)
load_dotenv(PROJECT_ROOT / ".env", override=False)

def _value(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"Missing required setting {name}. Copy example.env to .env and configure it."
        )
    return value.strip()


def _bool(name: str) -> bool:
    return _value(name).strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        value = int(_value(name))
    except ValueError as exc:
        raise RuntimeError(f"Setting {name} must be an integer") from exc
    if value < minimum or (maximum is not None and value > maximum):
        upper = f" and at most {maximum}" if maximum is not None else ""
        raise RuntimeError(f"Setting {name} must be at least {minimum}{upper}")
    return value


def _folder_name(name: str) -> str:
    value = _value(name)
    if value in {".", ".."} or "/" in value or "\\" in value or "\x00" in value:
        raise RuntimeError(f"Setting {name} must be a single folder name")
    return value


@dataclass(frozen=True)
class Settings:
    app_host: str = _value("APP_HOST")
    app_port: int = _int("APP_PORT")
    app_reload: bool = _bool("APP_RELOAD")
    debug: bool = _bool("DEBUG")
    secret_key: str = _value("SECRET_KEY")
    access_token_expire_minutes: int = _int("ACCESS_TOKEN_EXPIRE_MINUTES")
    remember_me_days: int = _int("REMEMBER_ME_DAYS", maximum=365)
    secure_cookies: bool = _bool("SECURE_COOKIES")
    database_path: str = _value("DATABASE_PATH")
    scheduler_db_path: str = _value("SCHEDULER_DB_PATH")
    vod_path: str = _value("VOD_PATH")
    vod_movies_folder: str = _folder_name("VOD_MOVIES_FOLDER")
    vod_series_folder: str = _folder_name("VOD_SERIES_FOLDER")
    vod_live_tv_folder: str = _folder_name("VOD_LIVE_TV_FOLDER")
    vod_unsorted_folder: str = _folder_name("VOD_UNSORTED_FOLDER")
    vod_unknown_year_folder: str = _folder_name("VOD_UNKNOWN_YEAR_FOLDER")
    m3u_dir: str = _value("M3U_DIR")
    vod_offline_path: str = _value("VOD_OFFLINE_PATH")
    log_dir: str = _value("LOG_DIR")
    timezone: str = _value("TZ")


settings = Settings()