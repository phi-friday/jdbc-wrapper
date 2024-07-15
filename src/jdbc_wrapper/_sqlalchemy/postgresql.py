# pyright: reportIncompatibleMethodOverride=false
# pyright: reportIncompatibleVariableOverride=false
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql.base import PGDialect
from typing_extensions import override

from jdbc_wrapper._sqlalchemy.connector import ConnectorSettings, JDBCConnector

if TYPE_CHECKING:
    from sqlalchemy.engine import ConnectArgsType, Connection
    from sqlalchemy.engine.url import URL


class PGJDBCDialect(JDBCConnector, PGDialect):
    settings = ConnectorSettings(
        jdbc_dsn_prefix="jdbc:postgresql://",
        name="postgresql",
        driver="jdbc_wrapper",
        inherit=PGDialect,
        is_async=False,
    )

    @override
    def initialize(self, connection: Connection) -> None:
        PGDialect.initialize(self, connection)

    @override
    def create_connect_args(self, url: URL) -> ConnectArgsType:
        dsn = self.jdbc_dsn_prefix
        if url.host:
            dsn += url.host
        if url.port:
            dsn += f":{url.port}"
        dsn += "/"
        if url.database:
            dsn += f"{url.database}"
        dsn += "?"

        query = dict(url.query)
        if url.username:
            query["user"] = url.username
        if url.password:
            query["password"] = url.password

        args = self._create_connect_args(dsn=dsn, query=query)
        return (), args


class AsyncPGJDBCDialect(PGJDBCDialect):
    settings = ConnectorSettings(
        jdbc_dsn_prefix="jdbc:postgresql://",
        name="postgresql",
        driver="jdbc_async_wrapper",
        inherit=PGJDBCDialect,
        is_async=True,
    )


dialect = PGJDBCDialect
dialect_async = AsyncPGJDBCDialect
