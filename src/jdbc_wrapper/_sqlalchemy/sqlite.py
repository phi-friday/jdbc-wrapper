# pyright: reportIncompatibleMethodOverride=false
# pyright: reportIncompatibleVariableOverride=false
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.util import memoized_property
from typing_extensions import override

from jdbc_wrapper._sqlalchemy.connector import (
    ConnectorSettings,
    DbapiModule,
    JDBCConnector,
)

if TYPE_CHECKING:
    from types import ModuleType

    from sqlalchemy.engine import ConnectArgsType, Connection
    from sqlalchemy.engine.url import URL


class SQJDBCDialect(JDBCConnector, SQLiteDialect):
    settings = ConnectorSettings(
        name="sqlite", driver="jdbc_wrapper", inherit=SQLiteDialect, is_async=False
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

    @override
    def create_connect_args(self, url: URL) -> ConnectArgsType:
        dsn = "jdbc:sqlite:"
        if url.database:
            database = Path(url.database).resolve()
            dsn += str(database)
        else:
            dsn += ":memory:"

        args = self._create_connect_args(dsn=dsn, query=url.query)
        return (), args


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
