# pyright: reportMissingImports=false
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from logging.config import dictConfig
from pathlib import Path
from typing import Any, Literal

from typing_extensions import override

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

__all__ = []

_LOG_TOML = Path(__file__).with_suffix(".toml")

_UTC = timezone(timedelta(0), "UTC")
_LTC = datetime.now(_UTC).astimezone().tzinfo or _UTC


class Formatter(logging.Formatter):
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        validate: bool = True,  # noqa: FBT001
        *,
        use_isoformat: bool = False,
        use_local_timezone: bool = False,
    ) -> None:
        super().__init__(fmt, datefmt, style, validate)
        self._use_isoformat = use_isoformat
        self._timezone = _LTC if use_local_timezone else _UTC

    @override
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        if not self._use_isoformat:
            return super().formatTime(record, datefmt)

        created_at = datetime.fromtimestamp(record.created, self._timezone)
        return created_at.isoformat()


def get_logger() -> logging.Logger:
    config = _load_config()
    _setup_config()
    name = config["default"]
    return logging.getLogger(name)


@lru_cache
def _load_config() -> dict[str, Any]:
    with _LOG_TOML.open("rb") as f:
        return tomllib.load(f)


def _setup_config() -> None:
    if getattr(_setup_config, "_flag", False) is True:
        return

    config = _load_config()
    dictConfig(config["config"])

    object.__setattr__(_setup_config, "_flag", True)
