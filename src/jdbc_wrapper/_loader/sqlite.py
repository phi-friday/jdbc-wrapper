from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, wait
from os import PathLike
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import override

from jdbc_wrapper._loader.github import GithubReleaseLoader
from jdbc_wrapper.const import (
    DEFAULT_SQLITE_JDBC_OWNER,
    DEFAULT_SQLITE_JDBC_REPO,
    SLF4J_JDBC_OWNER,
    SLF4J_JDBC_REPO,
    SLF4J_MAVEN_URL,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from os import PathLike
    from pathlib import Path

__all__ = []


class SQliteLoader(GithubReleaseLoader):
    def __init__(
        self,
        owner: str | None = None,
        repo: str | None = None,
        path: str | PathLike[str] | None = None,
        *,
        list_filter: Callable[[dict[str, Any]], bool] | None = None,
        asset_filter: Callable[[dict[str, Any]], bool] | None = None,
    ) -> None:
        if not owner or not repo:
            pass  # FIXME: log warning
        super().__init__(
            owner or "",
            repo or "",
            path,
            list_filter=list_filter,
            asset_filter=asset_filter,
        )

    @override
    def find_latest_local(self) -> str:
        if not self._owner or not self._repo:
            raise NotImplementedError
        return super().find_latest_local()

    @classmethod
    @override
    def find_latest_default(cls) -> str:
        new = cls(DEFAULT_SQLITE_JDBC_OWNER, DEFAULT_SQLITE_JDBC_REPO)
        return new.find_latest_local()

    @override
    def load_latest(self, path: str | PathLike[str] | None = None) -> Sequence[Path]:
        with ThreadPoolExecutor(2) as pool:
            future_self = pool.submit(super().load_latest, path)
            future_slf4j = pool.submit(super().load_latest, None)
            wait([future_self, future_slf4j], return_when="ALL_COMPLETED")
            return [*future_self.result(), *future_slf4j.result()]


class Slf4jLoader(GithubReleaseLoader):
    _stable_version_pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"^v_(?P<major>\d+)\.(?P<minor>\d+).(?P<patch>\d+)$"
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        super().__init__(SLF4J_JDBC_OWNER, SLF4J_JDBC_REPO, None)

    @property
    def _tag_list_url(self) -> str:
        return (
            f"{self._base_url}/repos/{self._owner}/{self._repo}"
            "/tags?page=1&per_page=10"
        )

    @property
    @override
    def _release_list_url(self) -> str:
        return self._tag_list_url

    @staticmethod
    def _create_slf4j_api_url(version: str | tuple[int, ...]) -> str:
        if isinstance(version, tuple):
            version = f"{version[0]}.{version[1]}.{version[2]}"
        return SLF4J_MAVEN_URL.format(version=version)

    @override
    def find_latest_local(self) -> str:
        raise NotImplementedError

    @classmethod
    @override
    def find_latest_default(cls) -> str:
        url = cls()._release_list_url  # noqa: SLF001
        request = cls._create_release_list_request(url)
        tag_list = cls._load_url_json(request)

        last_stable: tuple[int, ...] = sorted(
            (
                tuple(map(int, match.groups()))
                for x in tag_list
                if (match := cls._stable_version_pattern.match(x["name"]))
            ),
            reverse=True,
        )[0]

        return cls._create_slf4j_api_url(last_stable)
