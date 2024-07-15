from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Generic, Literal

from typing_extensions import Self, TypeVar, override

from jdbc_wrapper.abc import ConnectionABC, CursorABC
from jdbc_wrapper.utils import wrap_errors

if TYPE_CHECKING:
    from types import TracebackType

    from jpype import dbapi2 as jpype_dbapi2

    from jdbc_wrapper.types import Description, Query

__all__ = []

_R_co = TypeVar("_R_co", covariant=True, bound=tuple[Any, ...], default=tuple[Any, ...])
_R2 = TypeVar("_R2", bound=tuple[Any, ...])


class Cursor(CursorABC[_R_co], Generic[_R_co]):
    __slots__ = ("_connection", "_jpype_cursor")

    def __init__(
        self, connection: ConnectionABC[Self], jpype_cursor: jpype_dbapi2.Cursor
    ) -> None:
        self._connection = connection
        self._jpype_cursor = jpype_cursor

    @classmethod
    def construct(
        cls, connection: ConnectionABC[Any], jpype_cursor: jpype_dbapi2.Cursor
    ) -> CursorABC[Any]:
        return cls(connection, jpype_cursor)

    def _as_type(self, operation: Query[_R2]) -> CursorABC[_R2]:  # noqa: ARG002
        return self.construct(self._connection, self._jpype_cursor)

    @property
    @wrap_errors
    @override
    def description(self) -> Sequence[Description] | None:
        return self._jpype_cursor.description

    @property
    @wrap_errors
    @override
    def rowcount(self) -> int:
        return self._jpype_cursor.rowcount

    @wrap_errors
    @override
    def callproc(
        self, procname: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> None:
        if isinstance(parameters, Mapping):
            parameters = tuple(parameters.values())

        return self._jpype_cursor.callproc(
            procname, () if parameters is None else tuple(parameters)
        )

    @wrap_errors
    @override
    def close(self) -> None:
        self._jpype_cursor.close()

    @wrap_errors
    @override
    def _execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> CursorABC[Any]:
        if isinstance(parameters, Mapping):
            parameters = list(parameters.values())

        self._jpype_cursor.execute(operation, list(parameters) if parameters else None)
        return self

    @wrap_errors
    @override
    def _executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[Any]:
        if isinstance(seq_of_parameters, Mapping):
            seq_of_parameters = list(seq_of_parameters.values())

        self._jpype_cursor.executemany(
            operation, list(seq_of_parameters) if seq_of_parameters else None
        )
        return self

    @wrap_errors
    @override
    def fetchone(self) -> _R_co | None:
        fetch = self._jpype_cursor.fetchone()
        if fetch is None:
            return None
        return tuple(fetch)  # pyright: ignore[reportReturnType]

    @wrap_errors
    @override
    def fetchmany(self, size: int = -1) -> Sequence[_R_co]:
        fetch = self._jpype_cursor.fetchmany(size)
        return [tuple(row) for row in fetch]  # pyright: ignore[reportReturnType]

    @wrap_errors
    @override
    def fetchall(self) -> Sequence[_R_co]:
        fetch = self._jpype_cursor.fetchall()
        return [tuple(row) for row in fetch]  # pyright: ignore[reportReturnType]

    @wrap_errors
    @override
    def nextset(self) -> bool:
        return self._jpype_cursor.nextset() is True

    @property
    @wrap_errors
    @override
    def arraysize(self) -> int:
        return self._jpype_cursor.arraysize

    @arraysize.setter
    @wrap_errors
    @override
    def arraysize(self, size: int) -> None:
        self._jpype_cursor.arraysize = size

    @override
    def setinputsizes(self, sizes: Sequence[int]) -> None:
        raise NotImplementedError

    @override
    def setoutputsize(self, size: int, column: int = -1) -> None:
        raise NotImplementedError

    @property
    @override
    def rownumber(self) -> int:
        raise NotImplementedError

    @property
    @override
    def connection(self) -> ConnectionABC[Self]:
        return self._connection

    @override
    def scroll(
        self, value: int, mode: Literal["relative", "absolute"] = "relative"
    ) -> None:
        raise NotImplementedError

    @override
    def next(self) -> _R_co:
        raise NotImplementedError

    @override
    def __iter__(self) -> Iterator[_R_co]:
        return (tuple(x) for x in iter(self._jpype_cursor))  # pyright: ignore[reportReturnType]

    @property
    @override
    def is_closed(self) -> bool:
        return self._jpype_cursor._closed or self._jpype_cursor._jcx.isClosed()  # noqa: SLF001

    @property
    @wrap_errors
    @override
    def lastrowid(self) -> Any:
        return self._jpype_cursor.lastrowid

    @property
    @wrap_errors
    @override
    def thread_id(self) -> int:
        return self._jpype_cursor._thread  # noqa: SLF001

    @thread_id.setter
    @wrap_errors
    @override
    def thread_id(self, value: int) -> None:
        self._jpype_cursor._thread = value  # noqa: SLF001

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
