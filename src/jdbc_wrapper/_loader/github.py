from __future__ import annotations

import json
from operator import itemgetter
from os import PathLike
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.request import Request, urlopen

from typing_extensions import override

from jdbc_wrapper._loader.base import BaseLoader

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from http.client import HTTPResponse
    from os import PathLike
    from pathlib import Path

__all__ = []

_download_count_getter = itemgetter("download_count")
_created_at_getter = itemgetter("created_at")


class GithubReleaseLoader(BaseLoader):
    default_driver = "github"
    _base_url: ClassVar[str] = "https://api.github.com"

    def __init__(
        self,
        owner: str,
        repo: str,
        path: str | PathLike[str] | None = None,
        *,
        list_filter: Callable[[dict[str, Any]], bool] | None = None,
        asset_filter: Callable[[dict[str, Any]], bool] | None = None,
        base_dir: str | PathLike[str] | None = None,
    ) -> None:
        super().__init__(base_dir)
        self._owner = owner
        self._repo = repo
        self._path = path
        self._list_filter = list_filter
        self._asset_filter = asset_filter

    @property
    def _release_list_url(self) -> str:
        return (
            f"{self._base_url}/repos/{self._owner}/{self._repo}"
            "/releases?page=1&per_page=10"
        )

    @classmethod
    def _create_release_list_request(cls, url: str) -> Request:
        cls.validate_url(url)
        return Request(  # noqa: S310
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            method="GET",
        )

    @classmethod
    def _load_url_json(cls, request: Request) -> list[dict[str, Any]]:
        response: HTTPResponse
        with urlopen(request, timeout=10) as response:  # noqa: S310
            data = response.read()
        return json.loads(data)

    def _find_latest_local(self, url: str) -> str:
        request = self._create_release_list_request(url)
        release_list = self._load_url_json(request)

        release_gen = (
            x for x in release_list if not x["draft"] and not x["prerelease"]
        )
        if self._list_filter:
            release_gen = (x for x in release_gen if self._list_filter(x))
        release_list = sorted(release_gen, key=_created_at_getter, reverse=True)
        if not release_list:
            raise ValueError("No releases found")

        latest_release = release_list[0]
        assets_list = latest_release["assets"]
        assets_gen = (asset for asset in assets_list if asset["name"].endswith(".jar"))
        if self._asset_filter:
            assets_gen = (asset for asset in assets_gen if self._asset_filter(asset))
        assets_list = sorted(assets_gen, key=_download_count_getter, reverse=True)
        if not assets_list:
            raise ValueError("No assets found")

        major = assets_list[0]
        return major["browser_download_url"]

    @override
    def find_latest_local(self) -> str:
        return self._find_latest_local(self._release_list_url)

    @override
    def load_latest(self, path: str | PathLike[str] | None = None) -> Sequence[Path]:
        path = path or self._path
        return super().load_latest(path)
