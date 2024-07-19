from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from jpype import dbapi2 as jpype_dbapi2
from typing_extensions import Self, TypeVar, override

from jdbc_wrapper import exceptions
from jdbc_wrapper.abc import AsyncConnectionABC, ConnectionABC, CursorABC
from jdbc_wrapper.connection import connect as sync_connect
from jdbc_wrapper.cursor_async import AsyncCursor

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from os import PathLike
    from types import TracebackType


__all__ = []

_T = TypeVar("_T")


class AsyncConnection(AsyncConnectionABC[AsyncCursor[Any]]):
    __slots__ = ("_sync_connection",)

    Error = exceptions.Error
    Warning = exceptions.Warning
    InterfaceError = exceptions.InterfaceError
    DatabaseError = exceptions.DatabaseError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotsupportedError = exceptions.NotSupportedError

    def __init__(self, sync_connection: ConnectionABC[CursorABC[Any]]) -> None:
        self._sync_connection = sync_connection

    @override
    async def close(self) -> None:
        if self.is_closed:
            await asyncio.sleep(0)
            return
        await self._safe_run_in_thread(self._sync_connection.close)

    @override
    async def commit(self) -> None:
        await self._safe_run_in_thread(self._sync_connection.commit)

    @override
    async def rollback(self) -> None:
        await self._safe_run_in_thread(self._sync_connection.rollback)

    @override
    def cursor(self) -> AsyncCursor[Any]:
        sync_cursor = self._sync_connection.cursor()
        return AsyncCursor(self, sync_cursor)

    @property
    @override
    def autocommit(self) -> bool:
        return self._sync_connection.autocommit

    @autocommit.setter
    @override
    def autocommit(self, value: bool) -> None:
        self._sync_connection.autocommit = value

    @property
    @override
    def is_closed(self) -> bool:
        return self._sync_connection.is_closed

    @override
    async def __aenter__(self) -> Self:
        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    @override
    def __del__(self) -> None:
        with suppress(Exception):
            self._sync_connection.close()

    @property
    @override
    def _dsn(self) -> str:
        return self._sync_connection._dsn  # noqa: SLF001

    async def _safe_run_in_thread(
        self, func: Callable[..., _T], *args: Any, **kwargs: Any
    ) -> _T:
        from jdbc_wrapper.utils_async import ensure_close, greenlet_spawn

        if self.is_closed:
            if func.__name__ == "close":
                await asyncio.sleep(0)
                return None  # pyright: ignore[reportReturnType]
            raise exceptions.OperationalError("Connection is closed")

        try:
            return await greenlet_spawn(func, *args, **kwargs)
        except (jpype_dbapi2.Error, exceptions.Error):
            await asyncio.shield(ensure_close(self))
            raise


def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = None,
    driver_args: Mapping[str, Any] | None = None,
    adapters: Mapping[Any, Callable[[Any], Any]] | None = None,
    converters: Mapping[Any, Callable[[Any], Any]] | None = None,
) -> AsyncConnection:
    """Constructor for creating a connection to the database.

    Returns a Connection Object.
    It takes a number of parameters which are database dependent.
    """
    sync_connection = sync_connect(
        dsn, driver, modules, driver_args, adapters, converters
    )

    return AsyncConnection(sync_connection)
