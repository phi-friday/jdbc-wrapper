from __future__ import annotations

import tempfile
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.request import urlretrieve

if TYPE_CHECKING:
    from os import PathLike

__all__ = []


class BaseLoader(ABC):
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

    def load_latest(self, path: str | PathLike[str] | None = None) -> Path:
        latest = self.find_latest()
        if not latest.startswith("https://"):
            error_msg = f"URL must start with 'https://': {latest}"
            raise ValueError(error_msg)
        path = _create_temp_file() if path is None else Path(path)
        urlretrieve(latest, path)  # noqa: S310
        return path


def _create_temp_file() -> Path:
    temp_dir = tempfile.gettempdir()
    temp_path = Path(temp_dir) / uuid.uuid4().hex
    temp_path.touch(exist_ok=False)
    return temp_path
