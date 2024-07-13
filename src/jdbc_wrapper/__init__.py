from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

from jdbc_wrapper.connection import Connection
from jdbc_wrapper.connection import connect as _sync_connect
from jdbc_wrapper.connection_async import AsyncConnection
from jdbc_wrapper.connection_async import connect as _async_connect
from jdbc_wrapper.const import API_LEVEL as apilevel  # noqa: N811
from jdbc_wrapper.const import PARAM_STYLE as paramstyle  # noqa: N811
from jdbc_wrapper.const import THREAD_SAFETY as threadsafety  # noqa: N811
from jdbc_wrapper.cursor import Cursor
from jdbc_wrapper.cursor_async import AsyncCursor
from jdbc_wrapper.exceptions import (
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
from jdbc_wrapper.types import Binary, Datetime, Decimal, Float, Number, String, Text

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
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

__version__: str


@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: Mapping[str, Any] | None = ...,
    adapters: Mapping[Any, Callable[[Any], Any]] | None = None,
    converters: Mapping[Any, Callable[[Any], Any]] | None = None,
) -> Connection: ...
@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: Mapping[str, Any] | None = ...,
    adapters: Mapping[Any, Callable[[Any], Any]] | None = None,
    converters: Mapping[Any, Callable[[Any], Any]] | None = None,
    *,
    is_async: Literal[False],
) -> Connection: ...
@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: Mapping[str, Any] | None = ...,
    adapters: Mapping[Any, Callable[[Any], Any]] | None = None,
    converters: Mapping[Any, Callable[[Any], Any]] | None = None,
    *,
    is_async: Literal[True],
) -> AsyncConnection: ...
@overload
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = ...,
    driver_args: Mapping[str, Any] | None = ...,
    adapters: Mapping[Any, Callable[[Any], Any]] | None = None,
    converters: Mapping[Any, Callable[[Any], Any]] | None = None,
    *,
    is_async: bool = ...,
) -> Connection | AsyncConnection: ...
def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = None,
    driver_args: Mapping[str, Any] | None = None,
    adapters: Mapping[Any, Callable[[Any], Any]] | None = None,
    converters: Mapping[Any, Callable[[Any], Any]] | None = None,
    *,
    is_async: bool = False,
) -> Connection | AsyncConnection:
    """Constructor for creating a connection to the database.

    Returns a Connection Object.
    It takes a number of parameters which are database dependent.
    """
    if is_async:
        return _async_connect(dsn, driver, modules, driver_args, adapters, converters)
    return _sync_connect(dsn, driver, modules, driver_args, adapters, converters)


def __getattr__(name: str) -> Any:  # pragma: no cover
    from importlib.metadata import version

    if name == "__version__":
        return version("jdbc_wrapper")

    error_msg = f"The attribute named {name!r} is undefined."
    raise AttributeError(error_msg)
