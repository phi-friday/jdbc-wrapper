# pyright: reportIncompatibleMethodOverride=false
# pyright: reportIncompatibleVariableOverride=false
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.dialects.mssql.base import MSDialect
from typing_extensions import override

from jdbc_wrapper._sqlalchemy.connector import ConnectorSettings, JDBCConnector

if TYPE_CHECKING:
    from sqlalchemy.engine import ConnectArgsType, Connection
    from sqlalchemy.engine.url import URL


class MSJDBCDialect(JDBCConnector, MSDialect):
    settings = ConnectorSettings(
        name="mssql", driver="jdbc_wrapper", inherit=MSDialect, is_async=False
    )

    @override
    def initialize(self, connection: Connection) -> None:
        MSDialect.initialize(self, connection)

    @override
    def create_connect_args(self, url: URL) -> ConnectArgsType:
        dsn = "jdbc:sqlserver://"
        if url.host:
            dsn += url.host
        if url.port:
            dsn += f":{url.port}"
        dsn += ";"
        if url.database:
            dsn += f"databaseName={url.database};"

        query = dict(url.query)
        encrypt = query.pop("encrypt", None)
        if encrypt:
            dsn += f"encrypt={encrypt};"

        if url.username:
            query["user"] = url.username
        if url.password:
            query["password"] = url.password

        args = self._create_connect_args(dsn=dsn, query=query)
        return (), args


class AsyncMSJDBCDialect(MSJDBCDialect):
    settings = ConnectorSettings(
        name="mssql", driver="jdbc_async_wrapper", inherit=MSJDBCDialect, is_async=True
    )


dialect = MSJDBCDialect
dialect_async = AsyncMSJDBCDialect
