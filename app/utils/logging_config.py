import logging
import os
from logging.handlers import RotatingFileHandler

from app.utils.env import resolve_path

_LOG_DIR = resolve_path(os.getenv("LOG_DIR", "data/logs"))
_LOG_FILE = os.path.join(_LOG_DIR, "app.log")


def configure_logging(debug: bool = False) -> None:
    os.makedirs(_LOG_DIR, exist_ok=True)

    level = logging.DEBUG if debug else logging.INFO

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if root_logger.handlers:
        root_logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        _LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
