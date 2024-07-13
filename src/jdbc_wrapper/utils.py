# pyright: reportAttributeAccessIssue=false
from __future__ import annotations

from functools import partial, wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any

import jpype
from jpype import dbapi2 as jpype_dbapi2
from typing_extensions import TypeVar

from jdbc_wrapper import exceptions

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import PathLike

__all__ = []

_F = TypeVar("_F", bound="Callable[..., Any]")
_T = TypeVar("_T")


class Java:
    def __init__(self, jvm_path: str | PathLike[str] | None = None) -> None:
        self._jvm_path = jvm_path

    def start(self, *modules: str | PathLike[str], **kwargs: Any) -> None:
        if self.is_started():
            self.attach_thread()
            return

        if not modules:
            jpype.startJVM(str(self.jvm_path), **kwargs)
            return

        module = "-Djava.class.path=" + ":".join(
            str(Path(x).resolve()) for x in modules
        )
        jpype.startJVM(str(self.jvm_path), module, **kwargs)

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
        if not cls.is_started():
            return
        jpype.java.lang.Thread.attach()

    def get_connection(
        self, dsn: str, driver: str, *modules: str | PathLike[str], **driver_args: Any
    ) -> jpype_dbapi2.Connection:
        self.assert_started()
        self.load_class(driver, *modules)
        return catch_errors(
            jpype_dbapi2.connect, dsn, driver=driver, driver_args=driver_args
        )

    @staticmethod
    def load_class(name: str, *modules: str | PathLike[str]) -> Any:
        return load_class(name, *modules)


def parse_url(path: str | PathLike[str]) -> Any:  # jpype.JClass(java.net.URL)
    return jpype.java.nio.file.Paths.get(str(path)).toUri().toURL()


def load_class(name: str, *modules: str | PathLike[str]) -> Any:  # jpype.JClass
    url_array = None

    if modules:
        url_array = jpype.JArray(jpype.java.net.URL)(len(modules))
        for index, module in enumerate(modules):
            url_array[index] = parse_url(module)

    if url_array is None:
        return jpype.JClass(jpype.java.lang.Class.forName(name))

    return jpype.JClass(
        jpype.java.lang.Class.forName(
            name,
            True,  # noqa: FBT003
            jpype.java.net.URLClassLoader(url_array),
        )
    )


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


async def run_in_thread(func: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    import anyio

    func = partial(func, *args, **kwargs)
    return await anyio.to_thread.run_sync(func)
