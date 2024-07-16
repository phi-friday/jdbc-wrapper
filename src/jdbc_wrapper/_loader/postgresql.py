from __future__ import annotations

import re
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.request import urlretrieve

from typing_extensions import override

from jdbc_wrapper._loader.github import GithubReleaseLoader
from jdbc_wrapper.const import (
    DEFAULT_POSTGRESQL_JDBC_DRIVER,
    DEFAULT_POSTGRESQL_JDBC_OWNER,
    DEFAULT_POSTGRESQL_JDBC_REPO,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from os import PathLike

__all__ = []


class PostgresqlLoader(GithubReleaseLoader):
    _default_driver = DEFAULT_POSTGRESQL_JDBC_DRIVER
    _stable_version_pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)\.jar$"
    )
    _org_jar_url: ClassVar[str] = (
        "https://jdbc.postgresql.org/download/postgresql-{version}.jar"
    )

    def __init__(
        self,
        owner: str | None = None,
        repo: str | None = None,
        path: str | PathLike[str] | None = None,
        *,
        list_filter: Callable[[dict[str, Any]], bool] | None = None,
        asset_filter: Callable[[dict[str, Any]], bool] | None = None,
        base_dir: str | PathLike[str] | None = None,
    ) -> None:
        if not owner or not repo:
            pass  # FIXME: log warning
        super().__init__(
            owner or "",
            repo or "",
            path,
            list_filter=list_filter,
            asset_filter=asset_filter,
            base_dir=base_dir,
        )

    @override
    def find_latest_local(self) -> str:
        if not self._owner or not self._repo:
            raise NotImplementedError
        return super().find_latest_local()

    @classmethod
    @override
    def find_latest_default(cls) -> str:
        new = cls(DEFAULT_POSTGRESQL_JDBC_OWNER, DEFAULT_POSTGRESQL_JDBC_REPO)
        return new.find_latest_local()

    @override
    def load_latest(self, path: str | PathLike[str] | None = None) -> Sequence[Path]:
        latest = super().find_latest()
        version_match = self._stable_version_pattern.search(latest)
        if not version_match:
            error_msg = f"Invalid url: {latest}"
            raise ValueError(error_msg)

        version = tuple(map(int, version_match.groups()))
        version_str = f"{version[0]}.{version[1]}.{version[2]}"
        url = self._org_jar_url.format(version=version_str)

        self.validate_url(url)
        path = (
            self._create_temp_file(self._base_dir, ".jar")
            if path is None
            else Path(path)
        )
        urlretrieve(url, path)  # noqa: S310
        return [path]
