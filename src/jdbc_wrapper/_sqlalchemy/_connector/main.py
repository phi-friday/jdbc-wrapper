from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import make_url, pool
from sqlalchemy import util as sa_util
from sqlalchemy.connectors import Connector
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.interfaces import BindTyping, DBAPIConnection, IsolationLevel
from typing_extensions import override

from jdbc_wrapper._sqlalchemy._connector.config import (
    ConnectorSettings,
    JDBCConnectorMeta,
)
from jdbc_wrapper._sqlalchemy._connector.connection_async import (
    AsyncConnection,
    AsyncConnectionFallback,
)
from jdbc_wrapper._sqlalchemy._connector.utils_async import await_fallback
from jdbc_wrapper.abc import ConnectionABC
from jdbc_wrapper.connection import connect as sync_connect
from jdbc_wrapper.connection_async import connect as async_connect
from jdbc_wrapper.const import (
    JDBC_QUERY_DRIVER,
    JDBC_QUERY_DSN,
    JDBC_QUERY_MODULES,
    PARAM_STYLE,
)
from jdbc_wrapper.exceptions import OperationalError
from jdbc_wrapper.utils_async import await_ as jdbc_await

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy.engine.url import URL

    from jdbc_wrapper.abc import ConnectionABC, CursorABC


class DbapiModule(ModuleType):
    def __init__(self, jdbc_wrapper_module: ModuleType) -> None:
        self._module = jdbc_wrapper_module

    @override
    def __getattr__(self, name: str) -> Any:
        return getattr(self._module, name)

    @override
    def __dir__(self) -> list[str]:
        return dir(self._module)

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        is_async = kwargs.pop("is_async", False)
        if is_async:
            async_fallback = kwargs.pop("async_fallback", False)
            creator_fn = kwargs.pop("async_creator_fn", None)

            async_fallback = sa_util.asbool(async_fallback)
            if creator_fn is None:
                connection = async_connect(*args, **kwargs)
            else:
                connection = creator_fn(*args, **kwargs)
                if async_fallback:
                    connection = await_fallback(connection)
                else:
                    connection = jdbc_await(connection)

            if async_fallback:
                return AsyncConnectionFallback(self, connection)
            return AsyncConnection(self, connection)
        return sync_connect(*args, **kwargs)


class DefaultConnector(DefaultDialect, Connector):  # pyright: ignore[reportIncompatibleVariableOverride,reportIncompatibleMethodOverride]
    ...


class JDBCConnector(DefaultConnector, metaclass=JDBCConnectorMeta):
    _jdbc_wrapper_dialect_settings: ConnectorSettings
    settings = ConnectorSettings(
        jdbc_dsn_prefix="jdbc://",
        name="jdbc_wrapper_base_connector",
        driver="jdbc_wrapper_base_driver",
        inherit=DefaultConnector,
        supports_sane_rowcount=True,
        supports_sane_multi_rowcount=False,
        supports_native_decimal=True,
        bind_typing=BindTyping.NONE,
    )
    jdbc_dsn_prefix: str
    default_paramstyle = PARAM_STYLE

    @classmethod
    @override
    def import_dbapi(cls) -> ModuleType:
        import jdbc_wrapper

        return DbapiModule(jdbc_wrapper)

    @property
    @override
    def dbapi(self) -> ModuleType | None:
        return self.import_dbapi()

    @dbapi.setter
    @override
    def dbapi(self, value: Any) -> None: ...

    @classmethod
    @override
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        if cls.is_async:
            async_fallback = url.query.get("async_fallback", False)
            if sa_util.asbool(async_fallback):
                return pool.FallbackAsyncAdaptedQueuePool
            return getattr(cls, "poolclass", pool.AsyncAdaptedQueuePool)
        return getattr(cls, "poolclass", pool.QueuePool)

    @override
    def is_disconnect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        e: Exception,
        connection: ConnectionABC[Any] | None,
        cursor: CursorABC[Any] | None,
    ) -> bool:
        if connection is not None:
            return connection.is_closed
        if cursor is not None:
            return cursor.is_closed
        return False

    @override
    def set_isolation_level(
        self, dbapi_connection: DBAPIConnection, level: IsolationLevel
    ) -> None:
        if level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
        else:
            dbapi_connection.autocommit = False
            super().set_isolation_level(dbapi_connection, level)

    def _create_connect_args(
        self, dsn: str, query: Mapping[str, Any]
    ) -> dict[str, Any]:
        driver_args = dict(query)
        if JDBC_QUERY_DSN in driver_args:
            dsn = driver_args.pop(JDBC_QUERY_DSN)
            dsn = (
                self.dialect_description
                + "://"
                + dsn.removeprefix(self.jdbc_dsn_prefix).removeprefix("://")
            )
            url = make_url(dsn)
            url = url.set(query=driver_args | dict(url.query))
            return self.create_connect_args(url)[1]

        try:
            driver: str = driver_args.pop(JDBC_QUERY_DRIVER)
        except KeyError as exc:
            error_msg = (
                f"The `{JDBC_QUERY_DRIVER!s}` key is required in the query string"
            )
            raise OperationalError(error_msg) from exc

        result: dict[str, Any] = {
            "dsn": dsn,
            "driver": driver,
            "is_async": self.is_async,
        }

        modules = driver_args.pop(JDBC_QUERY_MODULES, None)
        result["driver_args"] = driver_args
        if modules:
            if isinstance(modules, str):
                modules = (modules,)
            result["modules"] = modules
        return result
