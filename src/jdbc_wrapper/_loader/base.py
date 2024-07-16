from __future__ import annotations

import os
import sys
import tempfile
import uuid
from abc import ABC, ABCMeta, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.request import urlretrieve

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import Sequence
    from os import PathLike

__all__ = []


class BaseLoaderMeta(ABCMeta):
    @property
    def default_driver(cls) -> str:
        return cls._loader_default_driver  # pyright: ignore[reportAttributeAccessIssue]

    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> Any:
        default_driver = namespace.pop("_default_driver", None)
        if default_driver:
            namespace["_loader_default_driver"] = default_driver

        return super().__new__(cls, name, bases, namespace, **kwargs)


class BaseLoader(ABC, metaclass=BaseLoaderMeta):
    default_driver: str

    _default_driver = "base"

    def __init__(self, base_dir: str | PathLike[str] | None = None) -> None:
        self._fix_macos_urllib_warning()
        self._base_dir = base_dir

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG003
        new = super().__new__(cls)
        object.__setattr__(new, "default_driver", cls.default_driver)
        return new

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
        path = (
            self._create_temp_file(self._base_dir, ".jar")
            if path is None
            else Path(path)
        )
        urlretrieve(latest, path)  # noqa: S310
        return [path]

    @staticmethod
    def _create_temp_file(
        base_dir: str | PathLike[str] | None, suffix: str | None
    ) -> Path:
        temp_dir = base_dir if base_dir else tempfile.gettempdir()
        temp_path = Path(temp_dir) / uuid.uuid4().hex
        if suffix:
            temp_path = temp_path.with_suffix(suffix)
        temp_path.touch(exist_ok=False)
        return temp_path
