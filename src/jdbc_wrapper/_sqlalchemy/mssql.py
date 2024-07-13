# pyright: reportIncompatibleMethodOverride=false
# pyright: reportIncompatibleVariableOverride=false
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.dialects.mssql.base import MSDialect
from typing_extensions import override

from jdbc_wrapper._sqlalchemy.base import DialectSettings, JDBCDialectBase

if TYPE_CHECKING:
    from sqlalchemy.engine import ConnectArgsType, Connection
    from sqlalchemy.engine.url import URL


class MSJDBCDialect(JDBCDialectBase, MSDialect):
    settings = DialectSettings(name="mssql", driver="mssql", inherit=MSDialect)

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
        if url.username:
            dsn += f"user={url.username};"
        if url.password:
            dsn += f"password={url.password};"

        query = dict(url.query)
        encrypt = query.pop("encrypt", None)
        if encrypt:
            dsn += f"encrypt={encrypt};"

        args = self._create_connect_args(dsn=dsn, query=query)
        return (), args


dialect = MSJDBCDialect
