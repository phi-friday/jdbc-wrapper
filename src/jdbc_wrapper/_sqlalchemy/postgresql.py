# pyright: reportIncompatibleMethodOverride=false
# pyright: reportIncompatibleVariableOverride=false
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql.base import PGDialect
from typing_extensions import override

from jdbc_wrapper._sqlalchemy.base import DialectSettings, JDBCDialectBase

if TYPE_CHECKING:
    from sqlalchemy.engine import ConnectArgsType, Connection
    from sqlalchemy.engine.url import URL


class PGJDBCDialect(JDBCDialectBase, PGDialect):
    settings = DialectSettings(
        name="postgresql", driver="jdbc_wrapper", inherit=PGDialect
    )

    @override
    def initialize(self, connection: Connection) -> None:
        PGDialect.initialize(self, connection)

    @override
    def create_connect_args(self, url: URL) -> ConnectArgsType:
        dsn = "jdbc:postgresql://"
        if url.host:
            dsn += url.host
        if url.port:
            dsn += f":{url.port}"
        dsn += "/"
        if url.database:
            dsn += f"{url.database}"
        dsn += "?"
        if url.username:
            dsn += f"user={url.username}&"
        if url.password:
            dsn += f"password={url.password}&"

        args = self._create_connect_args(dsn=dsn, query=url.query)
        return (), args


dialect = PGJDBCDialect
