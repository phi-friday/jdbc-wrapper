"""This module provides the Loader class for downloading jdbc drivers for each database.

However, it does not take responsibility for any bugs or issues
that may arise from its usage.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from jdbc_wrapper._loader.mssql import MssqlLoader
from jdbc_wrapper._loader.postgresql import PostgresqlLoader
from jdbc_wrapper._loader.sqlite import SQliteLoader

if TYPE_CHECKING:
    from collections.abc import Sequence
    from os import PathLike
    from pathlib import Path

    from jdbc_wrapper._loader.base import BaseLoader

__all__ = ["SQliteLoader", "PostgresqlLoader", "MssqlLoader", "load"]


def load(
    backend: str, base_dir: str | PathLike[str] | None = None, /
) -> tuple[str, Sequence[Path]]:
    backend = backend.lower()
    loader_class: type[BaseLoader]
    if backend in {"sqlite", SQliteLoader.default_driver.lower()}:
        loader_class = SQliteLoader
    elif backend in {"postgresql", PostgresqlLoader.default_driver.lower()}:
        loader_class = PostgresqlLoader
    elif backend in {"mssql", MssqlLoader.default_driver.lower()}:
        loader_class = MssqlLoader
    else:
        error_msg = f"Not found driver: {backend}"
        raise ValueError(error_msg)

    loader = loader_class(base_dir=base_dir)
    paths = loader.load_latest()
    return loader.default_driver, paths
