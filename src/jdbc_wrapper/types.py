from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal as _Decimal
from typing import TYPE_CHECKING, Any, Generic, cast

from jpype import dbapi2 as jpype_dbapi2
from jpype.dbapi2 import JDBCType
from typing_extensions import TypeAlias, TypeVar

from jdbc_wrapper.utils import wrap_errors

if TYPE_CHECKING:
    from collections.abc import Mapping

    from _typeshed import Incomplete

    from jdbc_wrapper.abc import TypePipeline

    class ResultSet:
        def __getattr__(self, name: str) -> Any: ...


__all__ = []

_J = TypeVar("_J", bound=JDBCType)
_T = TypeVar("_T")
_R = TypeVar("_R", bound=tuple[Any, ...], default=tuple[Any, ...])
_R2 = TypeVar("_R2", bound=tuple[Any, ...])

_registry = {}
_JDBCMeta: TypeAlias = "Incomplete"
_JDBCStmt: TypeAlias = "Incomplete"

Description: TypeAlias = """tuple[
    str, int | str | None, int | None, int | None, int | None, int | None, bool | None
]"""

TypeCodes: Mapping[str, int] = {}


class Query(Generic[_R]):
    __slots__ = ("_statement",)

    def __init__(self, statement: str) -> None:
        self._statement = statement

    @property
    def statement(self) -> str:
        return self._statement

    def as_type(self, dtype: type[_R2]) -> Query[_R2]:
        return self.construct(self._statement, dtype)

    @classmethod
    def construct(cls, statement: str, dtype: type[_R2]) -> Query[_R2]:
        return cls[dtype](statement)  # pyright: ignore[reportReturnType,reportInvalidTypeArguments]


class JDBCTypeGetter:
    __slots__ = ("_meta", "_index", "_connection")

    def __init__(
        self,
        meta: _JDBCMeta,
        index: int,
        connection: jpype_dbapi2.Connection | None = None,
    ) -> None:
        self._meta = meta
        self._index = index
        self._connection = connection

    @wrap_errors
    def using_type(self) -> JDBCType:
        return jpype_dbapi2.GETTERS_BY_TYPE(self._connection, self._meta, self._index)

    @wrap_errors
    def using_name(self) -> JDBCType:
        return jpype_dbapi2.GETTERS_BY_NAME(self._connection, self._meta, self._index)


class JDBCTypeSetter:
    __slots__ = ("_meta", "_index", "_python_type", "_connection")

    def __init__(
        self,
        meta: _JDBCMeta | None,
        index: int | None,
        python_type: type[Any] | None,
        connection: jpype_dbapi2.Connection | None = None,
    ) -> None:
        self._meta = meta
        self._index = index
        self._python_type = python_type
        self._connection = connection

    @wrap_errors
    def using_type(self) -> JDBCType | None:
        if self._python_type is None:
            raise AttributeError("Python type not set")
        return jpype_dbapi2.SETTERS_BY_TYPE(
            self._connection, self._meta, self._index, self._python_type
        )

    @wrap_errors
    def using_meta(self) -> JDBCType:
        if self._meta is None or self._index is None:
            raise AttributeError("Meta or index not set")
        return jpype_dbapi2.SETTERS_BY_META(
            self._connection, self._meta, self._index, self._python_type
        )


class WrappedJDBCType(Generic[_J, _T]):
    __slots__ = ("_jdbc_type", "_python_type")

    def __init__(self, jdbc_type: _J, python_type: type[_T] | TypePipeline[_T]) -> None:
        self._jdbc_type = jdbc_type
        self._python_type = python_type

        if self.name and self.type_code:
            type_codes = cast(dict[str, int], TypeCodes)
            type_codes.setdefault(self.name, self.type_code)

        _registry.setdefault(self._jdbc_type, self)

    @property
    def name(self) -> str | None:
        return self._jdbc_type._name  # noqa: SLF001

    @property
    def type_code(self) -> int | None:
        return self._jdbc_type._code  # noqa: SLF001

    @property
    def pipeline(self) -> TypePipeline[_T]:
        from jdbc_wrapper.pipeline import get_type_pipeline

        return get_type_pipeline(self._python_type)

    @wrap_errors
    def get_value(self, result_set: ResultSet, index: int, is_callable: bool) -> _T:  # noqa: FBT001
        return self._jdbc_type.get(result_set, index, is_callable)

    @wrap_errors
    def set_value(self, statement: _JDBCStmt, index: int, value: _T) -> Any:
        return self._jdbc_type.set(statement, index, value)

    def __repr__(self) -> str:
        return repr(self._jdbc_type)

    def __eq__(self, value: object) -> bool:
        return (
            value == self._jdbc_type
            if isinstance(value, WrappedJDBCType)
            else self._jdbc_type == value
        )

    def __hash__(self) -> int:
        return hash(self._jdbc_type)


### declare:: start
Array = WrappedJDBCType(jpype_dbapi2.ARRAY, list)
Bigint = WrappedJDBCType(jpype_dbapi2.BIGINT, int)
Bit = WrappedJDBCType(jpype_dbapi2.BIT, bool)
Blob = WrappedJDBCType(jpype_dbapi2.BLOB, bytes)
Boolean = WrappedJDBCType(jpype_dbapi2.BOOLEAN, bool)
Char = WrappedJDBCType(jpype_dbapi2.CHAR, str)
Clob = WrappedJDBCType(jpype_dbapi2.CLOB, str)
Date = WrappedJDBCType(jpype_dbapi2.DATE, date)
Double = WrappedJDBCType(jpype_dbapi2.DOUBLE, float)
Integer = WrappedJDBCType(jpype_dbapi2.INTEGER, int)
Object = WrappedJDBCType(jpype_dbapi2.OBJECT, object)
Longvarchar = WrappedJDBCType(jpype_dbapi2.LONGVARCHAR, str)
Longvarbinary = WrappedJDBCType(jpype_dbapi2.LONGVARBINARY, bytes)
Longvarchar = WrappedJDBCType(jpype_dbapi2.LONGVARCHAR, str)
Nchar = WrappedJDBCType(jpype_dbapi2.NCHAR, str)
Nclob = WrappedJDBCType(jpype_dbapi2.NCLOB, str)
Null: WrappedJDBCType[JDBCType, None] = WrappedJDBCType(jpype_dbapi2.NULL, type(None))
Numeric = WrappedJDBCType(jpype_dbapi2.NUMERIC, _Decimal)
Nvarchar = WrappedJDBCType(jpype_dbapi2.NVARCHAR, str)
Other = WrappedJDBCType(jpype_dbapi2.OTHER, object)
Real = WrappedJDBCType(jpype_dbapi2.REAL, float)
Ref = WrappedJDBCType(jpype_dbapi2.REF, object)
Rowid = WrappedJDBCType(jpype_dbapi2.ROWID, object)
Resultset = WrappedJDBCType(jpype_dbapi2.RESULTSET, object)
Smallint = WrappedJDBCType(jpype_dbapi2.SMALLINT, int)
Sqlxml = WrappedJDBCType(jpype_dbapi2.SQLXML, str)
Time = WrappedJDBCType(jpype_dbapi2.TIME, time)
Time_with_timezone = WrappedJDBCType(jpype_dbapi2.TIME_WITH_TIMEZONE, time)
Timestamp = WrappedJDBCType(jpype_dbapi2.TIMESTAMP, datetime)
Timestamp_with_timezone = WrappedJDBCType(
    jpype_dbapi2.TIMESTAMP_WITH_TIMEZONE, datetime
)
Tinyint = WrappedJDBCType(jpype_dbapi2.TINYINT, int)
Varbinary = WrappedJDBCType(jpype_dbapi2.VARBINARY, bytes)
Varchar = WrappedJDBCType(jpype_dbapi2.VARCHAR, str)
# alias
String = WrappedJDBCType(jpype_dbapi2.STRING, str)
Text = WrappedJDBCType(jpype_dbapi2.TEXT, str)
Binary = WrappedJDBCType(jpype_dbapi2.BINARY, bytes)
Number = WrappedJDBCType(jpype_dbapi2.NUMBER, int)
Float = WrappedJDBCType(jpype_dbapi2.FLOAT, float)
Decimal = WrappedJDBCType(jpype_dbapi2.DECIMAL, _Decimal)
Datetime = WrappedJDBCType(jpype_dbapi2.TIMESTAMP, datetime)
# special
Ascii_stream = WrappedJDBCType(jpype_dbapi2.ASCII_STREAM, str)
Binary_stream = WrappedJDBCType(jpype_dbapi2.BINARY_STREAM, bytes)
Character_stream = WrappedJDBCType(jpype_dbapi2.CHARACTER_STREAM, str)
Ncharacter_stream = WrappedJDBCType(jpype_dbapi2.NCHARACTER_STREAM, str)
Url = WrappedJDBCType(jpype_dbapi2.URL, str)
### declare:: end


def find_type_code(description: Description | int | str | None) -> int | None:
    if isinstance(description, tuple):
        description = description[1]

    if isinstance(description, int):
        return description

    if not description:
        return None

    return TypeCodes.get(description, None)


def get_wrapped_type(jdbc_type: JDBCType) -> WrappedJDBCType[JDBCType, Any]:
    return _registry[jdbc_type]
