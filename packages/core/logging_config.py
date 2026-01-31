from __future__ import annotations

import logging
import logging.config
import os
import sys
from typing import Optional


def _log_level() -> str:
    return os.getenv("LOG_LEVEL", "INFO").upper()


def _log_destination() -> str:
    return os.getenv("LOG_DESTINATION", "stdout").lower()


def _log_file_path() -> Optional[str]:
    return os.getenv("LOG_FILE")


def configure_logging() -> None:
    destination = _log_destination()
    handlers = {}

    if destination == "file":
        log_file = _log_file_path()
        if not log_file:
            raise RuntimeError("LOG_FILE is required when LOG_DESTINATION=file")
        handlers["default"] = {
            "class": "logging.FileHandler",
            "level": _log_level(),
            "filename": log_file,
            "formatter": "standard",
        }
    else:
        stream = sys.stdout if destination == "stdout" else sys.stderr
        handlers["default"] = {
            "class": "logging.StreamHandler",
            "level": _log_level(),
            "stream": stream,
            "formatter": "standard",
        }

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
                }
            },
            "handlers": handlers,
            "root": {"handlers": ["default"], "level": _log_level()},
        }
    )
