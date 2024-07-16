from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import override

from jdbc_wrapper._loader.github import GithubReleaseLoader
from jdbc_wrapper.const import (
    DEFAULT_POSTGRESQL_JDBC_DRIVER,
    DEFAULT_POSTGRESQL_JDBC_OWNER,
    DEFAULT_POSTGRESQL_JDBC_REPO,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import PathLike

__all__ = []


class PostgresqlLoader(GithubReleaseLoader):
    _default_driver = DEFAULT_POSTGRESQL_JDBC_DRIVER

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
