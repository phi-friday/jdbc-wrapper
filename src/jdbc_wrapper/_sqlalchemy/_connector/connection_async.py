from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_connection
from typing_extensions import Self, override

from jdbc_wrapper._sqlalchemy._connector.utils_async import await_fallback, await_only
from jdbc_wrapper.abc import AsyncConnectionABC, ConnectionABC

if TYPE_CHECKING:
    from types import ModuleType, TracebackType

    from jdbc_wrapper._sqlalchemy._connector.cursor_async import AsyncCursor

__all__ = []


class AsyncConnection(AsyncAdapt_dbapi_connection, ConnectionABC[Any]):
    __slots__ = ("dbapi", "_connection", "_execute_mutex")
    await_ = staticmethod(await_only)

    def __init__(self, dbapi: ModuleType, connection: AsyncConnectionABC[Any]) -> None:
        self.dbapi = dbapi
        self._connection = connection
        self._execute_mutex = asyncio.Lock()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    @property
    def execute_mutex(self) -> asyncio.Lock:
        return self._execute_mutex

    @property
    @override
    def _cursor_cls(self) -> type[AsyncCursor]:
        from jdbc_wrapper._sqlalchemy._connector.cursor_async import AsyncCursor

        return AsyncCursor

    @property
    @override
    def _ss_cursor_cls(self) -> type[AsyncCursor]:
        from jdbc_wrapper._sqlalchemy._connector.cursor_async import (
            AsyncCursorServerSide,
        )

        return AsyncCursorServerSide

    @override
    def ping(self, reconnect: bool) -> None:
        raise NotImplementedError

    @override
    def add_output_converter(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    @override
    def character_set_name(self) -> str:
        raise NotImplementedError

    @property
    @override
    def autocommit(self) -> bool:
        return self._connection.autocommit

    @autocommit.setter
    @override
    def autocommit(self, value: bool) -> None:
        self._connection.autocommit = value

    @property
    @override
    def is_closed(self) -> bool:
        return self._connection.is_closed

    @override
    def close(self) -> None:
        if self.is_closed:
            return None

        return super().close()

    @override
    def commit(self) -> None:
        if self.is_closed:
            return None

        return super().commit()

    @override
    def rollback(self) -> None:
        if self.is_closed:
            return None

        return super().rollback()

    @override
    def cursor(self, server_side: bool = False) -> AsyncCursor:
        if self.is_closed:
            raise self.dbapi.ProgrammingError("Attempt to use a closed connection.")

        if server_side:
            return self._ss_cursor_cls(self)
        return self._cursor_cls(self)

    @override
    def __enter__(self) -> Self:
        return self

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @override
    def __del__(self) -> None:
        with suppress(Exception):
            self.close()


class AsyncConnectionFallback(AsyncConnection):
    await_ = staticmethod(await_fallback)
