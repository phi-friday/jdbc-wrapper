from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

from pyjdbc2.connection import Connection
from pyjdbc2.connection import connect as _sync_connect
from pyjdbc2.connection_async import AsyncConnection
from pyjdbc2.connection_async import connect as _async_connect
from pyjdbc2.const import API_LEVEL as apilevel  # noqa: N811
from pyjdbc2.const import PARAM_STYLE as paramstyle  # noqa: N811
from pyjdbc2.const import THREAD_SAFETY as threadsafety  # noqa: N811
from pyjdbc2.cursor import Cursor
from pyjdbc2.cursor_async import AsyncCursor
from pyjdbc2.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pyjdbc2.types import Binary, Datetime, Decimal, Float, Number, String, Text

if TYPE_CHECKING:
    from collections.abc import Iterable
    from os import PathLike

__all__ = [
    # connect
    "connect",
    # global
    "apilevel",
    "threadsafety",
    "paramstyle",
    # connection
    "Connection",
    "AsyncConnection",
    # cursor
    "Cursor",
    "AsyncCursor",
    # error
    "DatabaseError",
    "DataError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    # type
    "Binary",
    "Datetime",
    "Decimal",
    "Float",
    "Number",
    "String",
    "Text",
]


@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: dict[str, Any] | None = ...,
) -> Connection: ...
@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: dict[str, Any] | None = ...,
    *,
    is_async: Literal[False],
) -> Connection: ...
@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: dict[str, Any] | None = ...,
    *,
    is_async: Literal[True],
) -> AsyncConnection: ...
@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: dict[str, Any] | None = ...,
    *,
    is_async: bool = ...,
) -> Connection | AsyncConnection: ...
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = None,
    driver_args: dict[str, Any] | None = None,
    *,
    is_async: bool = False,
) -> Connection | AsyncConnection:
    """Constructor for creating a connection to the database.

    Returns a Connection Object.
    It takes a number of parameters which are database dependent.
    """
    if is_async:
        return _async_connect(dsn, driver, modules, driver_args)
    return _sync_connect(dsn, driver, modules, driver_args)
