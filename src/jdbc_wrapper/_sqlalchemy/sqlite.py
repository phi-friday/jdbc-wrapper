# pyright: reportIncompatibleMethodOverride=false
# pyright: reportIncompatibleVariableOverride=false
from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.sqlite.base import DATE, DATETIME, TIME, SQLiteDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.types import Date, DateTime, Time
from sqlalchemy.util import memoized_property
from typing_extensions import override

from jdbc_wrapper._sqlalchemy.connector import (
    ConnectorSettings,
    DbapiModule,
    JDBCConnector,
)
from jdbc_wrapper.const import SQLITE_JDBC_PREFIX

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from types import ModuleType

    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.interfaces import Dialect
    from sqlalchemy.engine.url import URL


def _date_to_datetime(date_value: str | date | datetime) -> datetime:
    if isinstance(date_value, str):
        return datetime.fromisoformat(date_value)
    if isinstance(date_value, datetime):
        return date_value
    return datetime(date_value.year, date_value.month, date_value.day)  # noqa: DTZ001


def _date_to_date(date_value: str | date | datetime) -> date:
    if isinstance(date_value, str):
        return date.fromisoformat(date_value)
    if isinstance(date_value, datetime):
        return date_value.date()
    return date_value


def _date_to_time(date_value: str | time | datetime) -> time:
    if isinstance(date_value, str):
        return time.fromisoformat(date_value)
    if isinstance(date_value, datetime):
        return date_value.time()
    return date_value


def _as_iso_format(date_value: date | datetime | time) -> str:
    if isinstance(date_value, (date, time)):
        return date_value.isoformat()
    error_msg = f"Expected date or time, got {type(date_value)}"
    raise TypeError(error_msg)


class SQJDBCDateTime(DATETIME):
    @override
    def bind_processor(self, dialect: Dialect) -> Callable[[Any], Any] | None:
        return _as_iso_format

    @override
    def result_processor(
        self, dialect: Dialect, coltype: Any
    ) -> Callable[[Any], Any] | None:
        return _date_to_datetime


class SQJDBCDate(DATE):
    @override
    def bind_processor(self, dialect: Dialect) -> Callable[[Any], Any] | None:
        return _as_iso_format

    @override
    def result_processor(
        self, dialect: Dialect, coltype: Any
    ) -> Callable[[Any], Any] | None:
        return _date_to_date


class SQJDBCTime(TIME):
    @override
    def bind_processor(self, dialect: Dialect) -> Callable[[Any], Any] | None:
        return _as_iso_format

    @override
    def result_processor(
        self, dialect: Dialect, coltype: Any
    ) -> Callable[[Any], Any] | None:
        return _date_to_time


class SQJDBCDialect(JDBCConnector, SQLiteDialect):
    settings = ConnectorSettings(
        name="sqlite",
        driver="jdbc_wrapper",
        inherit=SQLiteDialect,
        colspecs=dict(SQLiteDialect.colspecs)
        | {DateTime: SQJDBCDateTime, Date: SQJDBCDate, Time: SQJDBCTime},
        is_async=False,
    )

    @property
    @override
    def dbapi(self) -> ModuleType | None:
        return None

    @dbapi.setter
    @override
    def dbapi(self, value: Any) -> None: ...

    @memoized_property
    def loaded_dbapi(self) -> ModuleType:
        return self.import_dbapi()

    @override
    def initialize(self, connection: Connection) -> None:
        SQLiteDialect.initialize(self, connection)

    @classmethod
    @override
    def parse_dsn_parts(cls, url: URL) -> tuple[str, Mapping[str, Any]]:
        dsn = "".join(SQLITE_JDBC_PREFIX)
        if url.database:
            database = Path(url.database).resolve()
            dsn += str(database)
        else:
            dsn += ":memory:"

        return dsn, url.query

    @classmethod
    @override
    def get_async_dialect_cls(cls, url: URL) -> type[AsyncSQJDBCDialect]:
        return AsyncSQJDBCDialect


class AsyncSQJDBCDialect(SQJDBCDialect):
    settings = ConnectorSettings(
        name="sqlite", driver="jdbc_async_wrapper", inherit=SQJDBCDialect, is_async=True
    )


class SQDbapiModule(DbapiModule):
    def __init__(self, module: DbapiModule) -> None:
        self._module = module

    @override
    def __getattr__(self, name: str) -> Any:
        return getattr(self._module, name)

    @override
    def __dir__(self) -> list[str]:
        return dir(self._module)


dialect = SQJDBCDialect
dialect_async = AsyncSQJDBCDialect
