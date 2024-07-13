from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any

from jpype import dbapi2 as jpype_dbapi2
from typing_extensions import Self, TypeVar, override

from pyjdbc2 import exceptions
from pyjdbc2.abc import AsyncConnectionABC, ConnectionABC, CursorABC
from pyjdbc2.connection import connect as sync_connect
from pyjdbc2.cursor_async import AsyncCursor
from pyjdbc2.utils import run_in_thread

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
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

    async def _safe_run_in_thread(
        self, func: Callable[..., _T], *args: Any, **kwargs: Any
    ) -> _T:
        import anyio

        if self.is_closed:
            raise exceptions.OperationalError("Connection is closed")

        try:
            return await run_in_thread(func, *args, **kwargs)
        except (jpype_dbapi2.Error, exceptions.Error):
            with anyio.CancelScope(shield=True):
                if not self.is_closed:
                    await self.close()
            raise


def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = None,
    driver_args: dict[str, Any] | None = None,
) -> AsyncConnection:
    """Constructor for creating a connection to the database.

    Returns a Connection Object.
    It takes a number of parameters which are database dependent.
    """
    sync_connection = sync_connect(dsn, driver, modules, driver_args)

    return AsyncConnection(sync_connection)
