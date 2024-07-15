from __future__ import annotations

from collections import deque
from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_cursor,
    AsyncAdapt_dbapi_ss_cursor,
)
from typing_extensions import Self, override

from jdbc_wrapper._sqlalchemy._connector.connection_async import AsyncConnection
from jdbc_wrapper._sqlalchemy._connector.utils_async import wrap_async
from jdbc_wrapper.abc import (
    AsyncConnectionABC,
    AsyncCursorABC,
    ConnectionABC,
    CursorABC,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence
    from types import TracebackType

    from jdbc_wrapper._sqlalchemy._connector.connection_async import AsyncConnection
    from jdbc_wrapper._sqlalchemy._connector.utils_async import AwaitFunc
    from jdbc_wrapper.types import Description


class AsyncCursor(AsyncAdapt_dbapi_cursor, CursorABC[Any]):
    __slots__ = ("_adapt_connection", "_connection", "await_", "_cursor", "_rows")
    server_side: ClassVar[bool] = False
    await_: AwaitFunc

    def __init__(self, connection: AsyncConnection) -> None:
        self._init_without_rows(connection)
        self._rows: deque[tuple[Any, ...]] = deque()

    def _init_without_rows(self, connection: AsyncConnection) -> None:
        self._adapt_connection: AsyncConnection = connection
        self._connection: AsyncConnectionABC[AsyncCursorABC[Any]] = (
            connection._connection  # noqa: SLF001
        )
        self.await_ = connection.await_

        cursor = self._connection.cursor()
        self._cursor: AsyncCursorABC[Any] = self._aenter_cursor(cursor)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    @property
    @override
    def description(self) -> Sequence[Description] | None:
        return self._cursor.description

    @property
    @override
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @wrap_async
    @override
    async def callproc(
        self, procname: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> None:
        return await self._cursor.callproc(procname, parameters)

    @override
    def close(self) -> None:
        self._rows.clear()

    @wrap_async
    @override
    async def _execute(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        await self._cursor.execute(operation, parameters)

    @wrap_async
    @override
    async def _executemany(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> None:
        await self._cursor.executemany(operation, seq_of_parameters)

    @override
    def fetchone(self) -> Any | None:
        return super().fetchone()

    @override
    def fetchmany(self, size: int | None = None) -> Sequence[Any]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return super().fetchmany(size)

    @override
    def fetchall(self) -> Sequence[Any]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return super().fetchall()

    @override
    def nextset(self) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        return super().nextset()

    @property
    @override
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    @override
    def arraysize(self, size: int) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self._cursor.arraysize = size

    @wrap_async
    @override
    async def setinputsizes(self, sizes: Sequence[Any], *args: Any) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        return await self._cursor.setinputsizes(sizes)

    @wrap_async
    @override
    async def setoutputsize(self, size: int, column: int = -1) -> None:
        return await self._cursor.setoutputsize(size, column)

    @property
    @override
    def rownumber(self) -> int:
        return self._cursor.rownumber

    @property
    @override
    def connection(self) -> ConnectionABC[Any]:
        return self._connection

    @wrap_async
    @override
    async def scroll(
        self, value: int, mode: Literal["relative", "absolute"] = "relative"
    ) -> None:
        return await self._cursor.scroll(value, mode)

    @wrap_async
    @override
    async def next(self) -> Any:
        return await self._cursor.next()

    @override
    def __iter__(self) -> Generator[Any, None, None]:  # noqa: PYI058
        yield from super().__iter__()

    @property
    @override
    def lastrowid(self) -> Any:
        return self._cursor.lastrowid

    @property
    @override
    def is_closed(self) -> bool:
        return self._cursor.is_closed

    @property
    @override
    def thread_id(self) -> int:
        return self._cursor.thread_id

    @thread_id.setter
    @override
    def thread_id(self, value: int) -> None:
        self._cursor.thread_id = value

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
            self._cursor._sync_cursor.close()  # type: ignore # noqa: SLF001


class AsyncCursorServerSide(AsyncAdapt_dbapi_ss_cursor, AsyncCursor):
    __slots__ = ("_adapt_connection", "_connection", "await_", "_cursor")
    server_side = True

    def __init__(self, connection: AsyncConnection) -> None:
        self._init_without_rows(connection)

    @override
    def close(self) -> None:
        return super().close()

    @override
    def fetchone(self) -> Any | None:
        return super().fetchone()

    @override
    def fetchmany(self, size: int | None = None) -> Sequence[Any]:
        return super().fetchmany(size)

    @override
    def fetchall(self) -> Sequence[Any]:
        return super().fetchall()
