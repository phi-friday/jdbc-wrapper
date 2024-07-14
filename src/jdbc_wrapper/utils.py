# pyright: reportAttributeAccessIssue=false
from __future__ import annotations

from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any

import jpype
from jpype import dbapi2 as jpype_dbapi2
from typing_extensions import TypeVar

from jdbc_wrapper import exceptions

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from os import PathLike

__all__ = []

_F = TypeVar("_F", bound="Callable[..., Any]")
_T = TypeVar("_T")

_driver_registry: dict[str, Any] = {}


class Java:
    def __init__(self, jvm_path: str | PathLike[str] | None = None) -> None:
        self._jvm_path = jvm_path

    def start(self, *modules: str | PathLike[str], **kwargs: Any) -> None:
        if self.is_started():
            self.attach()
            return

        self.add_modules(*modules)
        jpype.startJVM(jvmpath=self.jvm_path, **kwargs)
        self.attach()

    def attach(self) -> None:
        if not self.is_started():
            self.start()

        if self.attached:
            return

        self.attach_thread()

    def add_modules(self, *modules: str | PathLike[str]) -> None:
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
            thread.attach()

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
        driver_manager = jpype.java.sql.DriverManager
        jdbc_driver: Any = None
        if driver in _driver_registry:
            jdbc_driver = _driver_registry[driver]
        else:
            for driver_instance in driver_manager.getDrivers():
                if type(driver_instance).__name__ == driver:
                    jdbc_driver = driver_instance
                    break
            else:
                jdbc_driver = self.load_driver_instance(driver, *modules)
                driver_manager.registerDriver(jdbc_driver)
            _driver_registry.setdefault(driver, jdbc_driver)

        info = jpype.java.util.Properties()
        for key, value in driver_args.items():
            info.setProperty(key, value)

        jdbc_connection = jdbc_driver.connect(dsn, info)
        return catch_errors(
            jpype_dbapi2.Connection,
            jdbc_connection,
            adapters=adapters,
            converters=converters,
            getters=jpype_dbapi2.GETTERS_BY_TYPE,
            setters=jpype_dbapi2.SETTERS_BY_TYPE,
        )

    @staticmethod
    def load_driver_instance(name: str, *modules: str | PathLike[str]) -> Any:
        return load_driver_instance(name, *modules)


def parse_url(path: str | PathLike[str]) -> Any:
    return jpype.java.nio.file.Paths.get(str(path)).toUri().toURL()


def load_driver_instance(name: str, *modules: str | PathLike[str]) -> Any:
    url_array = None

    if modules:
        url_array = jpype.JArray(jpype.java.net.URL)(len(modules))
        for index, module in enumerate(modules):
            url_array[index] = parse_url(module)

    if url_array is None:
        driver_class = jpype.java.lang.Class.forName(name)
    else:
        driver_class = jpype.java.lang.Class.forName(
            name,
            True,  # noqa: FBT003
            jpype.java.net.URLClassLoader(url_array),
        )
    return driver_class.newInstance()


def wrap_errors(func: _F) -> _F:
    @wraps(func)
    def inner(*args: Any, **kwargs: Any) -> Any:
        return catch_errors(func, *args, **kwargs)

    return inner  # pyright: ignore[reportReturnType]


def catch_errors(func: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:  # noqa: C901
    try:
        return func(*args, **kwargs)
    except jpype_dbapi2.NotSupportedError as exc:
        raise exceptions.NotSupportedError(*exc.args) from exc
    except jpype_dbapi2.ProgrammingError as exc:
        raise exceptions.ProgrammingError(*exc.args) from exc
    except jpype_dbapi2.InternalError as exc:
        raise exceptions.InternalError(*exc.args) from exc
    except jpype_dbapi2.IntegrityError as exc:
        raise exceptions.IntegrityError(*exc.args) from exc
    except jpype_dbapi2.OperationalError as exc:
        raise exceptions.OperationalError(*exc.args) from exc
    except jpype_dbapi2.DataError as exc:
        raise exceptions.DataError(*exc.args) from exc
    except jpype_dbapi2.DatabaseError as exc:
        raise exceptions.DatabaseError(*exc.args) from exc
    except jpype_dbapi2.InterfaceError as exc:
        raise exceptions.InterfaceError(*exc.args) from exc
    except jpype_dbapi2.Warning as exc:
        raise exceptions.Warning(*exc.args) from exc
    except jpype_dbapi2.Error as exc:
        raise exceptions.Error(*exc.args) from exc
