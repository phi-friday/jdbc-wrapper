from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal as _Decimal
from typing import TYPE_CHECKING, Any, Generic

from jpype import dbapi2 as jpype_dbapi2
from jpype.dbapi2 import JDBCType
from typing_extensions import TypeAlias, TypeVar

from pyjdbc2.utils import wrap_errors

if TYPE_CHECKING:
    from collections.abc import Callable

    from _typeshed import Incomplete

    class ResultSet:
        def __getattr__(self, name: str) -> Any: ...


__all__ = []

_J = TypeVar("_J", bound=JDBCType)
_T = TypeVar("_T")
_R = TypeVar("_R", bound=tuple[Any, ...], default=tuple[Any, ...])
_R2 = TypeVar("_R2", bound=tuple[Any, ...])

_JDBCMeta: TypeAlias = "Incomplete"
_JDBCStmt: TypeAlias = "Incomplete"


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


class BaseType(Generic[_J, _T]):
    __slots__ = ("_jdbc_type", "_python_type")

    def __init__(
        self, jdbc_type: _J, python_type: Callable[[Any], _T] | type[_T]
    ) -> None:
        self._jdbc_type = jdbc_type
        self._python_type = python_type

    @property
    def type_code(self) -> int | None:
        return self._jdbc_type._code  # noqa: SLF001

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
            if isinstance(value, BaseType)
            else self._jdbc_type == value
        )

    def __hash__(self) -> int:
        return hash(self._jdbc_type)


def _return_self(obj: _T) -> _T:
    return obj


### declare:: start
Array = BaseType(jpype_dbapi2.ARRAY, list)
Bigint = BaseType(jpype_dbapi2.BIGINT, int)
Bit = BaseType(jpype_dbapi2.BIT, bool)
Blob = BaseType(jpype_dbapi2.BLOB, bytes)
Boolean = BaseType(jpype_dbapi2.BOOLEAN, bool)
Char = BaseType(jpype_dbapi2.CHAR, str)
Clob = BaseType(jpype_dbapi2.CLOB, str)
Date = BaseType(jpype_dbapi2.DATE, date)
Double = BaseType(jpype_dbapi2.DOUBLE, float)
Integer = BaseType(jpype_dbapi2.INTEGER, int)
Object = BaseType(jpype_dbapi2.OBJECT, _return_self)
Longvarchar = BaseType(jpype_dbapi2.LONGVARCHAR, str)
Longvarbinary = BaseType(jpype_dbapi2.LONGVARBINARY, bytes)
Longvarchar = BaseType(jpype_dbapi2.LONGVARCHAR, str)
Nchar = BaseType(jpype_dbapi2.NCHAR, str)
Nclob = BaseType(jpype_dbapi2.NCLOB, str)
Null: BaseType[JDBCType, None] = BaseType(jpype_dbapi2.NULL, _return_self)
Numeric = BaseType(jpype_dbapi2.NUMERIC, _Decimal)
Nvarchar = BaseType(jpype_dbapi2.NVARCHAR, str)
Other = BaseType(jpype_dbapi2.OTHER, _return_self)
Real = BaseType(jpype_dbapi2.REAL, float)
Ref = BaseType(jpype_dbapi2.REF, _return_self)  # FIXME
Rowid = BaseType(jpype_dbapi2.ROWID, _return_self)  # FIXME
Resultset = BaseType(jpype_dbapi2.RESULTSET, _return_self)  # FIXME
Smallint = BaseType(jpype_dbapi2.SMALLINT, int)
Sqlxml = BaseType(jpype_dbapi2.SQLXML, str)
Time = BaseType(jpype_dbapi2.TIME, time)
Time_with_timezone = BaseType(jpype_dbapi2.TIME_WITH_TIMEZONE, time)
Timestamp = BaseType(jpype_dbapi2.TIMESTAMP, datetime)
Timestamp_with_timezone = BaseType(jpype_dbapi2.TIMESTAMP_WITH_TIMEZONE, datetime)
Tinyint = BaseType(jpype_dbapi2.TINYINT, int)
Varbinary = BaseType(jpype_dbapi2.VARBINARY, bytes)
Varchar = BaseType(jpype_dbapi2.VARCHAR, str)
# alias
String = BaseType(jpype_dbapi2.STRING, str)
Text = BaseType(jpype_dbapi2.TEXT, str)
Binary = BaseType(jpype_dbapi2.BINARY, bytes)
Number = BaseType(jpype_dbapi2.NUMBER, int)
Float = BaseType(jpype_dbapi2.FLOAT, float)
Decimal = BaseType(jpype_dbapi2.DECIMAL, _Decimal)
Datetime = BaseType(jpype_dbapi2.TIMESTAMP, datetime)
# special
Ascii_stream = BaseType(jpype_dbapi2.ASCII_STREAM, str)
Binary_stream = BaseType(jpype_dbapi2.BINARY_STREAM, bytes)
Character_stream = BaseType(jpype_dbapi2.CHARACTER_STREAM, str)
Ncharacter_stream = BaseType(jpype_dbapi2.NCHARACTER_STREAM, str)
Url = BaseType(jpype_dbapi2.URL, str)
### declare:: end
