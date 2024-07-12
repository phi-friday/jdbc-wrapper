from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any

from typing_extensions import Self, override

from pyjdbc2 import exceptions
from pyjdbc2.abc import ConnectionABC
from pyjdbc2.cursor import Cursor
from pyjdbc2.utils import Java, wrap_errors

if TYPE_CHECKING:
    from collections.abc import Iterable
    from os import PathLike
    from types import TracebackType

    from jpype import dbapi2 as jpype_dbapi2


__all__ = []


class Connection(ConnectionABC[Cursor[Any]]):
    __slots__ = ("_jpype_connection",)

    Error = exceptions.Error
    Warning = exceptions.Warning
    InterfaceError = exceptions.InterfaceError
    DatabaseError = exceptions.DatabaseError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotsupportedError = exceptions.NotSupportedError

    def __init__(self, jpype_connection: jpype_dbapi2.Connection) -> None:
        self._jpype_connection = jpype_connection

    @wrap_errors
    @override
    def close(self) -> None:
        self._jpype_connection.close()

    @wrap_errors
    @override
    def commit(self) -> None:
        self._jpype_connection.commit()

    @wrap_errors
    @override
    def rollback(self) -> None:
        self._jpype_connection.rollback()

    @wrap_errors
    @override
    def cursor(self) -> Cursor[Any]:
        jpype_cursor = self._jpype_connection.cursor()
        return Cursor(self, jpype_cursor)

    @property
    @wrap_errors
    def autocommit(self) -> bool:
        return self._jpype_connection.autocommit

    @autocommit.setter
    @wrap_errors
    def autocommit(self, value: bool) -> None:
        self._jpype_connection.autocommit = value

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()


def connect(
    dsn: str,
    driver: str,
    modules: Iterable[str | PathLike[str]] | None = None,
    driver_args: dict[str, Any] | None = None,
) -> Connection:
    """Constructor for creating a connection to the database.

    Returns a Connection Object.
    It takes a number of parameters which are database dependent.
    """
    jvm = Java()
    jvm.start(*(() if modules is None else modules))
    jpype_connection = jvm.get_connection(dsn, driver, **(driver_args or {}))

    return Connection(jpype_connection)
