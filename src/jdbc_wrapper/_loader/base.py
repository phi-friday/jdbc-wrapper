from __future__ import annotations

import os
import sys
import tempfile
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.request import urlretrieve

if TYPE_CHECKING:
    from collections.abc import Sequence
    from os import PathLike

__all__ = []


class BaseLoader(ABC):
    def __init__(self) -> None:
        self._fix_macos_urllib_warning()

    def _fix_macos_urllib_warning(self) -> None:
        if sys.platform.startswith("darwin"):
            os.environ.setdefault("no_proxy", "*")

    def find_latest_local(self) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def find_latest_default(cls) -> str: ...

    def find_latest(self) -> str:
        try:
            return self.find_latest_local()
        except NotImplementedError:
            return self.find_latest_default()

    @staticmethod
    def validate_url(url: str) -> None:
        if not url.startswith("https://"):
            error_msg = f"URL must start with 'https://': {url}"
            raise ValueError(error_msg)

    def load_latest(self, path: str | PathLike[str] | None = None) -> Sequence[Path]:
        latest = self.find_latest()
        self.validate_url(latest)
        path = _create_temp_file(".jar") if path is None else Path(path)
        urlretrieve(latest, path)  # noqa: S310
        return [path]


def _create_temp_file(suffix: str | None) -> Path:
    temp_dir = tempfile.gettempdir()
    temp_path = Path(temp_dir) / uuid.uuid4().hex
    if suffix:
        temp_path = temp_path.with_suffix(suffix)
    temp_path.touch(exist_ok=False)
    return temp_path
