# pyright: reportUnknownLambdaType=false
# ruff: noqa: E731
from __future__ import annotations

from abc import ABCMeta
from collections.abc import Callable, Iterator, Mapping
from datetime import date, datetime, time
from decimal import Decimal
from itertools import chain
from typing import Any, ClassVar, Generic, overload

import jpype
from typing_extensions import TypeVar, override

from jdbc_wrapper.abc import TypePipeline

try:
    from sqlalchemy import quoted_name
except ImportError:  # pragma: no cover
    quoted_name = str

__all__ = []

_registry = {}
_registry_queue = set()
_T = TypeVar("_T")


class LazyMapping(Mapping[Any, Callable[[Any], Any]]):
    __slots__ = ("_other",)
    _func_name: ClassVar[str]

    def __init__(self, other: Mapping[Any, Callable[[Any], Any]] | None) -> None:
        self._other = other or {}

    @override
    def __getitem__(self, key: Any) -> Callable[[Any], Any]:
        maybe = self._other.get(key)
        if maybe is not None:
            return maybe
        return getattr(get_type_pipeline(key), self._func_name)

    @override
    def __iter__(self) -> Iterator[Any]:
        registry = set()
        for key in chain(self._other, _registry):
            if key in registry:
                continue
            registry.add(key)
            yield key

    @override
    def __len__(self) -> int:
        return len(set(chain(self._other, _registry)))


class LazyConvertor(LazyMapping):
    _func_name = "java_to_python"


class LazyAdapter(LazyMapping):
    _func_name = "python_to_java"


class PipelineMeta(ABCMeta):
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> Any:
        jtypes = namespace.pop("jtypes", None)
        if callable(jtypes):
            namespace["jtypes"] = staticmethod(jtypes)
        elif jtypes is not None:
            namespace["jtypes"] = jtypes
        return super().__new__(cls, name, bases, namespace, **kwargs)


class BasePipeline(TypePipeline[_T], Generic[_T], metaclass=PipelineMeta):
    dtypes: tuple[type[_T] | str, ...]
    jtypes: Callable[[], tuple[Any, ...]] | tuple[Any, ...] = ()

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        _registry_queue.add(cls)

    def __init__(self) -> None:
        for dtype in self.dtypes:
            _registry.setdefault(dtype, self)
            dtype_str = dtype if isinstance(dtype, str) else dtype.__qualname__
            _registry.setdefault(dtype_str.upper(), self)

        jtypes = self.jtypes() if callable(self.jtypes) else self.jtypes
        for jtype in jtypes:
            _registry.setdefault(jtype, self)


class ObjectPipeline(BasePipeline[object]):
    dtypes = (object,)
    jtypes = lambda: (jpype.JObject, jpype.java.lang.Object)

    @override
    def java_to_python(self, value: Any) -> object:
        if isinstance(value, jpype.java.lang.Object):
            return _get_python_object(value)
        return value

    @override
    def python_to_java(self, value: object) -> Any:
        return value


class NullPipeline(BasePipeline[None]):
    dtypes = (type(None),)

    @override
    def java_to_python(self, value: Any) -> None:
        return None

    @override
    def python_to_java(self, value: None) -> Any:
        return value


class BooleanPipeline(BasePipeline[bool]):
    dtypes = (bool,)
    jtypes = lambda: (jpype.JBoolean, jpype.java.lang.Boolean)

    @override
    def java_to_python(self, value: Any) -> bool:
        if isinstance(value, jpype.java.lang.Object):
            return _get_python_object(value)
        return bool(value)

    @override
    def python_to_java(self, value: bool) -> Any:
        return value


class StrPipeline(BasePipeline[str]):
    dtypes = (str, quoted_name)
    jtypes = lambda: (
        jpype.JString,
        jpype.JChar,
        jpype.java.lang.String,
        jpype.java.sql.Clob,
        quoted_name,
    )

    @override
    def java_to_python(self, value: Any) -> str:
        return str(value)

    @override
    def python_to_java(self, value: str) -> Any:
        return value


class BinaryPipeline(BasePipeline[bytes]):
    dtypes = (bytes,)
    jtypes = lambda: (jpype.JByte, jpype.JArray(jpype.JByte), jpype.java.sql.Blob)

    @override
    def java_to_python(self, value: Any) -> bytes:
        return bytes(value)

    @override
    def python_to_java(self, value: bytes) -> Any:
        return value


class IntPipeline(BasePipeline[int]):
    dtypes = (int,)
    jtypes = (jpype.JInt, jpype.JLong, jpype.JShort)

    @override
    def java_to_python(self, value: Any) -> int:
        return int(value)

    @override
    def python_to_java(self, value: int) -> Any:
        return value


class FloatPipeline(BasePipeline[float]):
    dtypes = (float,)
    jtypes = (jpype.JFloat, jpype.JDouble)

    @override
    def java_to_python(self, value: Any) -> float:
        return float(value)

    @override
    def python_to_java(self, value: float) -> Any:
        return value


class DatePipeline(BasePipeline[date]):
    dtypes = (date,)
    jtypes = lambda: (jpype.java.sql.Date,)

    @override
    def java_to_python(self, value: Any) -> date:
        if isinstance(value, (str, jpype.JString, jpype.java.lang.String)):
            return date.fromisoformat(value)
        if isinstance(value, jpype.java.lang.Object):
            return _get_python_object(value)
        return value

    @override
    def python_to_java(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return value


class TimePipeline(BasePipeline[time]):
    dtypes = (time,)
    jtypes = lambda: (jpype.java.sql.Time,)

    @override
    def java_to_python(self, value: Any) -> time:
        if isinstance(value, (str, jpype.JString, jpype.java.lang.String)):
            return time.fromisoformat(value)
        if isinstance(value, jpype.java.lang.Object):
            return _get_python_object(value)
        return value

    @override
    def python_to_java(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.time().isoformat()
        if isinstance(value, time):
            return value.isoformat()
        return value


class DatetimePipeline(BasePipeline[datetime]):
    dtypes = (datetime,)
    jtypes = lambda: (jpype.java.sql.Timestamp,)

    @override
    def java_to_python(self, value: Any) -> datetime:
        if isinstance(value, (str, jpype.JString, jpype.java.lang.String)):
            return datetime.fromisoformat(value)
        if isinstance(value, jpype.java.lang.Object):
            return _get_python_object(value)
        return value

    @override
    def python_to_java(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day).isoformat()  # noqa: DTZ001
        return value


class DecimalPipeline(BasePipeline[Decimal]):
    dtypes = (Decimal,)
    jtypes = lambda: (jpype.java.math.BigDecimal,)

    @override
    def java_to_python(self, value: Any) -> Decimal:
        if isinstance(value, jpype.java.lang.Object):
            return _get_python_object(value)
        return Decimal(value)

    @override
    def python_to_java(self, value: Decimal) -> Any:
        return float(value)


@overload
def get_type_pipeline(dtype: TypePipeline[_T]) -> TypePipeline[_T]: ...
@overload
def get_type_pipeline(dtype: type[_T]) -> TypePipeline[_T]: ...
@overload
def get_type_pipeline(dtype: str) -> TypePipeline[Any]: ...
@overload
def get_type_pipeline(
    dtype: TypePipeline[Any] | type[Any] | str,
) -> TypePipeline[Any]: ...
def get_type_pipeline(dtype: TypePipeline[Any] | type[Any] | str) -> TypePipeline[Any]:
    if isinstance(dtype, TypePipeline):
        return dtype
    if isinstance(dtype, str):
        return _registry[dtype.upper()]
    return _registry[dtype]


def _get_python_object(value: Any) -> Any:
    return value._py()  # noqa: SLF001


def _init_pipeline() -> None:
    for cls in _registry_queue:
        cls()


jpype._jinit.registerJVMInitializer(_init_pipeline)  # noqa: SLF001
