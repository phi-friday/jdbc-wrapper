# pyright: reportAttributeAccessIssue=false
from __future__ import annotations

import logging
from functools import wraps
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any

import jpype
from jpype import dbapi2 as jpype_dbapi2
from typing_extensions import TypeGuard, TypeVar

from jdbc_wrapper import const as jdbc_wrapper_const
from jdbc_wrapper import exceptions, types

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, MutableSequence
    from os import PathLike

    from sqlalchemy.engine.url import URL
    from sqlalchemy.sql.base import Executable
    from sqlalchemy.sql.elements import CompilerElement

    from jdbc_wrapper._sqlalchemy.connector import JDBCConnector

    class ExecutableAndCompiled(Executable, CompilerElement): ...


__all__ = []

_F = TypeVar("_F", bound="Callable[..., Any]")
_T = TypeVar("_T")

_driver_registry: dict[str, Any] = {}
logger = logging.getLogger("jdbc_wrapper")
_USE_SQLALCHEMY = find_spec("sqlalchemy") is not None


class Java:
    def __init__(self, jvm_path: str | PathLike[str] | None = None) -> None:
        self._jvm_path = jvm_path

    def start(self, *modules: str | PathLike[str], **kwargs: Any) -> None:
        if self.is_started():
            logger.debug("JVM is already started. Attaching to JVM.")
            self.attach()
            return

        self.add_modules(*modules)
        jpype.startJVM(jvmpath=self.jvm_path, **kwargs)
        self.attach()

    def attach(self) -> None:
        if not self.is_started():
            logger.debug("JVM is not started. Starting JVM.")
            self.start()

        if self.attached:
            logger.debug("Thread is already attached to JVM.")
            return

        self.attach_thread()

    def shutdown(self) -> None:
        if not self.is_started():
            logger.debug("JVM is not started. Nothing to shutdown.")
            return
        logger.warning(
            "Due to limitations in the JPype, JVM cannot be restarted after shutdown."
            "see more: https://github.com/jpype-project/jpype/issues/959"
        )
        jpype.shutdownJVM()

    def add_modules(self, *modules: str | PathLike[str]) -> None:
        if self.is_started():
            logger.warning("Cannot add modules after JVM is started.")
            return

        for module in modules:
            path = Path(module).resolve()
            jpype.addClassPath(path)

    @property
    def java(self) -> jpype.JPackage:
        return jpype.java

    @property
    def javax(self) -> jpype.JPackage:
        return jpype.javax

    @property
    def attached(self) -> bool:
        if not self.is_started():
            return False
        return jpype.java.lang.Thread.isAttached()

    @property
    def modules(self) -> set[str]:
        return set(jpype.getClassPath().split(":"))

    @property
    def jvm_path(self) -> Path:
        if not self._jvm_path:
            self._jvm_path = jpype.getDefaultJVMPath()
        return Path(self._jvm_path)

    @staticmethod
    def is_started() -> bool:
        return jpype.isJVMStarted()

    @classmethod
    def assert_started(cls) -> None:
        if not cls.is_started():
            raise RuntimeError("JVM is not started")

    @classmethod
    def attach_thread(cls) -> None:
        thread = jpype.java.lang.Thread
        if not thread.isAttached():
            thread.attachAsDaemon()

    def get_connection(
        self,
        dsn: str,
        driver: str,
        *modules: str | PathLike[str],
        driver_args: Mapping[str, Any],
        adapters: Mapping[Any, Callable[[Any], Any]] | None = None,
        converters: Mapping[Any, Callable[[Any], Any]] | None = None,
    ) -> jpype_dbapi2.Connection:
        from jdbc_wrapper.pipeline import LazyAdapter, LazyConvertor

        adapters = LazyAdapter(adapters)
        converters = LazyConvertor(converters)

        self.assert_started()
        self.attach()
        driver_manager = jpype.java.sql.DriverManager
        jdbc_driver: Any = None
        if driver in _driver_registry:
            logger.debug("Using cached JDBC driver: %s", driver)
            jdbc_driver = _driver_registry[driver]
        else:
            for driver_instance in driver_manager.getDrivers():
                if type(driver_instance).__name__ == driver:
                    logger.debug("Found existing JDBC driver: %s", driver)
                    jdbc_driver = driver_instance
                    break
            else:
                logger.debug("Loading JDBC driver: %s", driver)
                jdbc_driver = self.load_driver_instance(driver, *modules)
                logger.debug("Registering JDBC driver: %s", driver)
                driver_manager.registerDriver(jdbc_driver)
            _driver_registry.setdefault(driver, jdbc_driver)

        info = jpype.java.util.Properties()
        for key, value in driver_args.items():
            info.setProperty(key, value)

        jdbc_connection = jdbc_driver.connect(dsn, info)
        use_dsn = types.UseDsn(dsn)
        return catch_errors(
            jpype_dbapi2.Connection,
            jdbc_connection,
            adapters=adapters,
            converters=converters,
            getters=use_dsn.getter,
            setters=use_dsn.setter,
        )

    @staticmethod
    def load_driver_instance(name: str, *modules: str | PathLike[str]) -> Any:
        return load_driver_instance(name, *modules)


def parse_url(path: str | PathLike[str]) -> Any:
    return jpype.java.nio.file.Paths.get(str(path)).toUri().toURL()


def load_driver_instance(name: str, *modules: str | PathLike[str]) -> Any:
    if modules:
        url_array: MutableSequence[Any]
        url_array = jpype.JArray(jpype.java.net.URL)(len(modules))
        for index, module in enumerate(modules):
            url_array[index] = parse_url(module)

        driver_class = jpype.java.lang.Class.forName(
            name,
            True,  # noqa: FBT003
            jpype.java.net.URLClassLoader(url_array),
        )
    else:
        driver_class = jpype.java.lang.Class.forName(name)

    return driver_class.newInstance()


def wrap_errors(func: _F) -> _F:
    @wraps(func)
    def inner(*args: Any, **kwargs: Any) -> Any:
        return catch_errors(func, *args, **kwargs)

    return inner  # pyright: ignore[reportReturnType]


def catch_errors(func: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    try:
        return func(*args, **kwargs)
    except (exceptions.Error, exceptions.Warning):
        raise
    except (jpype_dbapi2.Error, jpype_dbapi2.Warning) as exc:
        name = type(exc).__qualname__.split(".")[-1]
        error_type = getattr(exceptions, name, exceptions.Error)
        error = error_type(*exc.args)
        raise error.with_traceback(exc.__traceback__) from exc.__cause__


def find_connector_type(dsn: str) -> type[JDBCConnector]:
    if dsn.startswith(jdbc_wrapper_const.SQLITE_JDBC_PREFIX[0]):
        from jdbc_wrapper._sqlalchemy.sqlite import SQJDBCDialect

        return SQJDBCDialect
    if dsn.startswith(jdbc_wrapper_const.POSTGRESQL_DSN_PREFIX[0]):
        from jdbc_wrapper._sqlalchemy.postgresql import PGJDBCDialect

        return PGJDBCDialect
    if dsn.startswith(jdbc_wrapper_const.MSSQL_DSN_PREFIX[0]):
        from jdbc_wrapper._sqlalchemy.mssql import MSJDBCDialect

        return MSJDBCDialect

    error_msg = f"Unknown JDBC DSN: {dsn}"
    raise ValueError(error_msg)


def dsn_to_url(
    dsn: str, connector: type[JDBCConnector] | JDBCConnector | None = None
) -> URL:
    from sqlalchemy.engine.url import make_url

    if connector is None:
        connector = find_connector_type(dsn)

    if connector.name == "sqlite":
        dsn_prefix = "".join(jdbc_wrapper_const.SQLITE_JDBC_PREFIX)
    elif connector.name == "postgresql":
        dsn_prefix = "".join(jdbc_wrapper_const.POSTGRESQL_DSN_PREFIX)
    elif connector.name == "mssql":
        dsn_prefix = "".join(jdbc_wrapper_const.MSSQL_DSN_PREFIX)
    else:
        error_msg = f"Unknown JDBC DSN: {dsn}"
        raise ValueError(error_msg)

    url_parts = dsn.removeprefix(dsn_prefix)

    url = connector.name + "+" + connector.driver + "://" + url_parts
    return make_url(url)


def url_to_dsn(
    url: str | URL, connector: type[JDBCConnector] | JDBCConnector | None = None
) -> tuple[str, Mapping[str, Any]]:
    from sqlalchemy.engine.url import make_url

    url = make_url(url)
    if connector is None:
        backend = url.get_backend_name()
        if backend == "sqlite":
            from jdbc_wrapper._sqlalchemy.sqlite import (
                SQJDBCDialect as connector,  # noqa: N813
            )
        elif backend == "postgresql":
            from jdbc_wrapper._sqlalchemy.postgresql import (
                PGJDBCDialect as connector,  # noqa: N813
            )
        elif backend == "mssql":
            from jdbc_wrapper._sqlalchemy.mssql import (
                MSJDBCDialect as connector,  # noqa: N813
            )
        else:
            error_msg = f"Unknown JDBC URL: {url}"
            raise ValueError(error_msg)

    return connector.parse_dsn_parts(url)


def is_sqlalchemy_executable(statement: Any) -> TypeGuard[ExecutableAndCompiled]:
    if not _USE_SQLALCHEMY:
        return False

    from sqlalchemy.sql.base import Executable
    from sqlalchemy.sql.elements import CompilerElement

    return isinstance(statement, Executable) and isinstance(statement, CompilerElement)


def statement_to_query(statement: Any, dsn: str) -> tuple[str, dict[str, Any]]:
    if not is_sqlalchemy_executable(statement):
        error_msg = "Statement is not an SQLAlchemy executable."
        raise TypeError(error_msg)

    connector_type = find_connector_type(dsn)
    connector = connector_type()
    compiled = statement.compile(dialect=connector)
    return str(compiled), dict(compiled.params or {})
