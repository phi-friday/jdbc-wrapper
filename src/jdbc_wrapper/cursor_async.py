from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Generic, Literal

from jpype import dbapi2 as jpype_dbapi2
from typing_extensions import Self, TypeVar, override

from jdbc_wrapper import exceptions
from jdbc_wrapper.abc import AsyncConnectionABC, AsyncCursorABC, CursorABC

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from types import TracebackType

    from sqlalchemy.sql import Executable

    from jdbc_wrapper.types import Description, Query

__all__ = []

_T = TypeVar("_T")
_F = TypeVar("_F", bound="Callable[..., Any]")
_R_co = TypeVar("_R_co", covariant=True, bound=tuple[Any, ...], default=tuple[Any, ...])
_R2 = TypeVar("_R2", bound=tuple[Any, ...])


class AsyncCursor(AsyncCursorABC[_R_co], Generic[_R_co]):
    __slots__ = ("_connection", "_sync_cursor")

    def __init__(
        self, connection: AsyncConnectionABC[Self], sync_cursor: CursorABC[_R_co]
    ) -> None:
        self._connection = connection
        self._sync_cursor = sync_cursor

    @classmethod
    def construct(
        cls, connection: AsyncConnectionABC[Any], sync_cursor: CursorABC[_R_co]
    ) -> AsyncCursorABC[Any]:
        return cls(connection, sync_cursor)

    def _as_type(self, operation: Query[_R2]) -> AsyncCursorABC[_R2]:  # noqa: ARG002
        return self.construct(self.connection, self._sync_cursor)

    @property
    @override
    def description(self) -> Sequence[Description] | None:
        return self._sync_cursor.description

    @property
    @override
    def rowcount(self) -> int:
        return self._sync_cursor.rowcount

    @override
    async def callproc(
        self, procname: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> None:
        return await self._safe_run_in_thread(
            self._sync_cursor.callproc, procname, parameters
        )

    @override
    async def close(self) -> None:
        if self.is_closed:
            await asyncio.sleep(0)
            return None
        return await self._safe_run_in_thread(self._sync_cursor.close)

    @override
    async def _execute(
        self,
        operation: str | Executable,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncCursorABC[Any]:
        await self._safe_run_in_thread(self._sync_cursor.execute, operation, parameters)
        return self

    @override
    async def _executemany(
        self,
        operation: str | Executable,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> AsyncCursorABC[Any]:
        await self._safe_run_in_thread(
            self._sync_cursor.executemany, operation, seq_of_parameters
        )
        return self

    @override
    async def fetchone(self) -> _R_co | None:
        return await self._safe_run_in_thread(self._sync_cursor.fetchone)

    @override
    async def fetchmany(self, size: int | None = None) -> Sequence[_R_co]:
        size = size or self.arraysize
        return await self._safe_run_in_thread(self._sync_cursor.fetchmany, size)

    @override
    async def fetchall(self) -> Sequence[_R_co]:
        return await self._safe_run_in_thread(self._sync_cursor.fetchall)

    @override
    async def nextset(self) -> bool:
        return await self._safe_run_in_thread(self._sync_cursor.nextset)

    @property
    @override
    def arraysize(self) -> int:
        return self._sync_cursor.arraysize

    @arraysize.setter
    @override
    def arraysize(self, size: int) -> None:
        self._sync_cursor.arraysize = size

    @override
    async def setinputsizes(self, sizes: Sequence[int]) -> None:
        raise NotImplementedError

    @override
    async def setoutputsize(self, size: int, column: int = -1) -> None:
        raise NotImplementedError

    @property
    @override
    def rownumber(self) -> int:
        raise NotImplementedError

    @property
    @override
    def connection(self) -> AsyncConnectionABC[Self]:
        return self._connection

    @override
    async def scroll(
        self, value: int, mode: Literal["relative", "absolute"] = "relative"
    ) -> None:
        raise NotImplementedError

    @override
    async def next(self) -> _R_co:
        raise NotImplementedError

    @override
    def __aiter__(self) -> Self:
        return self

    @property
    @override
    def lastrowid(self) -> Any:
        return self._sync_cursor.lastrowid

    @property
    @override
    def is_closed(self) -> bool:
        return self._sync_cursor.is_closed

    @property
    @override
    def thread_id(self) -> int:
        return self._sync_cursor.thread_id

    @thread_id.setter
    @override
    def thread_id(self, value: int) -> None:
        self._sync_cursor.thread_id = value

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
            self._sync_cursor.close()

    async def _safe_run_in_thread(
        self, func: Callable[..., _T], *args: Any, **kwargs: Any
    ) -> _T:
        from jdbc_wrapper.utils_async import ensure_close, greenlet_spawn

        if self.is_closed:
            if func.__name__ == "close":
                await asyncio.sleep(0)
                return None  # pyright: ignore[reportReturnType]
            raise exceptions.OperationalError("Cursor is closed")

        wrapped = self._wrap_thread_id_error(func)

        try:
            return await greenlet_spawn(wrapped, *args, **kwargs)
        except (jpype_dbapi2.Error, exceptions.Error):
            await asyncio.shield(ensure_close(self))
            raise

    def _wrap_thread_id_error(self, func: _F) -> _F:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            origin = self.thread_id
            try:
                self.thread_id = threading.get_ident()
                result = func(*args, **kwargs)
            finally:
                self.thread_id = origin
            return result

        return wrapper  # pyright: ignore[reportReturnType]
