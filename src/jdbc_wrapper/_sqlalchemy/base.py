from __future__ import annotations

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import suppress
from dataclasses import MISSING, asdict, dataclass, fields
from functools import wraps
from types import ModuleType, TracebackType
from typing import TYPE_CHECKING, Any, Literal

import greenlet
from sqlalchemy import pool
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.interfaces import AdaptedConnection, BindTyping, Dialect
from typing_extensions import ParamSpec, Self, TypeVar, override

from jdbc_wrapper.abc import (
    AsyncConnectionABC,
    AsyncCursorABC,
    ConnectionABC,
    CursorABC,
)
from jdbc_wrapper.connection import connect as sync_connect
from jdbc_wrapper.connection_async import connect as async_connect
from jdbc_wrapper.exceptions import OperationalError
from jdbc_wrapper.utils_async import await_, check_async_greenlet, check_in_sa_greenlet

if TYPE_CHECKING:
    from collections.abc import (
        Awaitable,
        Callable,
        Coroutine,
        Iterator,
        Mapping,
        MutableMapping,
        Sequence,
    )

    from sqlalchemy.engine.url import URL
    from sqlalchemy.sql.compiler import InsertmanyvaluesSentinelOpts
    from sqlalchemy.types import TypeEngine

    from jdbc_wrapper.types import Description

_T = TypeVar("_T")
_P = ParamSpec("_P")

_dataclass_options = {"frozen": True}
if sys.version_info >= (3, 10):
    _dataclass_options["kw_only"] = True
    _dataclass_options["slots"] = False

unset: Any = object()


@dataclass(**_dataclass_options)
class DialectSettings:
    """Settings for a dialect.

    see more:
        `:ref:sqlalchemy.engine.interface.Dialect`
        `:ref:sqlalchemy.engine.default.DefaultDialect`
    """

    name: str
    """identifying name for the dialect from a DBAPI-neutral point of view
    (i.e. 'sqlite')
    """

    driver: str
    """identifying name for the dialect's DBAPI"""

    inherit: type[Dialect] | None = None

    supports_alter: bool = unset
    """``True`` if the database supports ``ALTER TABLE`` - used only for
    generating foreign key constraints in certain circumstances
    """

    supports_comments: bool = unset
    """Indicates the dialect supports comment DDL on tables and columns."""

    supports_constraint_comments: bool = unset
    """Indicates if the dialect supports comment DDL on constraints."""

    inline_comments: bool = unset
    """Indicates the dialect supports comment DDL that's inline with the
    definition of a Table or Column.  If False, this implies that ALTER must
    be used to set table and column comments."""

    supports_statement_cache: bool = unset
    """indicates if this dialect supports caching.

    All dialects that are compatible with statement caching should set this
    flag to True directly on each dialect class and subclass that supports
    it.  SQLAlchemy tests that this flag is locally present on each dialect
    subclass before it will use statement caching.  This is to provide
    safety for legacy or new dialects that are not yet fully tested to be
    compliant with SQL statement caching.
    """

    div_is_floordiv: bool = unset
    """target database treats the / division operator as "floor division" """

    bind_typing: BindTyping = unset
    """define a means of passing typing information to the database and/or
    driver for bound parameters.
    """

    include_set_input_sizes: set[Any] | None = unset
    """set of DBAPI type objects that should be included in
    automatic cursor.setinputsizes() calls.
    """
    exclude_set_input_sizes: set[Any] | None = unset
    """set of DBAPI type objects that should be excluded in
    automatic cursor.setinputsizes() calls.
    """

    default_sequence_base: int = unset
    """the default value that will be rendered as the "START WITH" portion of
    a CREATE SEQUENCE DDL statement.
    """

    execute_sequence_format: type[tuple[Any, ...] | tuple[list[Any], ...]] = unset
    """either the 'tuple' or 'list' type, depending on what cursor.execute()
    accepts for the second argument (they vary)."""

    supports_sequences: bool = unset
    """Indicates if the dialect supports CREATE SEQUENCE or similar."""

    sequences_optional: bool = unset
    """If True, indicates if the :paramref:`_schema.Sequence.optional`
    parameter on the :class:`_schema.Sequence` construct
    should signal to not generate a CREATE SEQUENCE. Applies only to
    dialects that support sequences. Currently used only to allow PostgreSQL
    SERIAL to be used on a column that specifies Sequence() for usage on
    other backends.
    """

    preexecute_autoincrement_sequences: bool = unset
    """True if 'implicit' primary key functions must be executed separately
    in order to get their value, if RETURNING is not used.

    This is currently oriented towards PostgreSQL when the
    ``implicit_returning=False`` parameter is used on a :class:`.Table`
    object.
    """

    supports_identity_columns: bool = unset
    """target database supports IDENTITY"""

    favor_returning_over_lastrowid: bool = unset
    """for backends that support both a lastrowid and a RETURNING insert
    strategy, favor RETURNING for simple single-int pk inserts.

    cursor.lastrowid tends to be more performant on most backends.
    """

    update_returning: bool = unset
    """if the dialect supports RETURNING with UPDATE"""

    delete_returning: bool = unset
    """if the dialect supports RETURNING with DELETE"""

    update_returning_multifrom: bool = unset
    """if the dialect supports RETURNING with UPDATE..FROM"""

    delete_returning_multifrom: bool = unset
    """if the dialect supports RETURNING with DELETE..FROM"""

    insert_returning: bool = unset
    """if the dialect supports RETURNING with INSERT"""

    cte_follows_insert: bool = unset
    """target database, when given a CTE with an INSERT statement, needs
    the CTE to be below the INSERT"""

    supports_native_enum: bool = unset
    """Indicates if the dialect supports a native ENUM construct.
    This will prevent :class:`_types.Enum` from generating a CHECK
    constraint when that type is used in "native" mode.
    """

    supports_native_boolean: bool = unset
    """Indicates if the dialect supports a native boolean construct.
    This will prevent :class:`_types.Boolean` from generating a CHECK
    constraint when that type is used.
    """

    supports_native_uuid: bool = unset
    """indicates if Python UUID() objects are handled natively by the
    driver for SQL UUID datatypes.
    """

    returns_native_bytes: bool = unset
    """indicates if Python bytes() objects are returned natively by the
    driver for SQL "binary" datatypes.
    """

    supports_simple_order_by_label: bool = unset
    """target database supports ORDER BY <labelname>, where <labelname>
    refers to a label in the columns clause of the SELECT"""

    tuple_in_values: bool = unset
    """target database supports tuple IN, i.e. (x, y) IN ((q, p), (r, z))"""

    engine_config_types: Mapping[str, Any] = unset
    """a mapping of string keys that can be in an engine config linked to
    type conversion functions.
    """

    supports_native_decimal: bool = unset
    """indicates if Decimal objects are handled and returned for precision
    numeric types, or if floats are returned"""

    max_identifier_length: int = unset
    """The maximum length of identifier names."""

    supports_sane_rowcount: bool = unset
    """Indicate whether the dialect properly implements rowcount for
    ``UPDATE`` and ``DELETE`` statements.
    """

    supports_sane_multi_rowcount: bool = unset
    """Indicate whether the dialect properly implements rowcount for
    ``UPDATE`` and ``DELETE`` statements when executed via
    executemany.
    """

    colspecs: MutableMapping[type[TypeEngine[Any]], type[TypeEngine[Any]]] = unset
    """A dictionary of TypeEngine classes from sqlalchemy.types mapped
    to subclasses that are specific to the dialect class.  This
    dictionary is class-level only and is not accessed from the
    dialect instance itself.
    """

    supports_default_values: bool = unset
    """dialect supports INSERT... DEFAULT VALUES syntax"""

    supports_default_metavalue: bool = unset
    """dialect supports INSERT...(col) VALUES (DEFAULT) syntax."""

    default_metavalue_token: str = unset
    """for INSERT... VALUES (DEFAULT) syntax, the token to put in the
    parenthesis.

    E.g. for SQLite this is the keyword "NULL".
    """

    supports_empty_insert: bool = unset
    """dialect supports INSERT () VALUES (), i.e. a plain INSERT with no
    columns in it.

    This is not usually supported; an "empty" insert is typically
    suited using either "INSERT..DEFAULT VALUES" or
    "INSERT ... (col) VALUES (DEFAULT)".
    """

    supports_multivalues_insert: bool = unset
    """Target database supports INSERT...VALUES with multiple value
    sets, i.e. INSERT INTO table (cols) VALUES (...), (...), (...), ...
    """

    use_insertmanyvalues: bool = unset
    """if True, indicates "insertmanyvalues" functionality should be used
    to allow for ``insert_executemany_returning`` behavior, if possible.

    In practice, setting this to True means:

    if ``supports_multivalues_insert``, ``insert_returning`` and
    ``use_insertmanyvalues`` are all True, the SQL compiler will produce
    an INSERT that will be interpreted by the :class:`.DefaultDialect`
    as an :attr:`.ExecuteStyle.INSERTMANYVALUES` execution that allows
    for INSERT of many rows with RETURNING by rewriting a single-row
    INSERT statement to have multiple VALUES clauses, also executing
    the statement multiple times for a series of batches when large numbers
    of rows are given.

    The parameter is False for the default dialect, and is set to
    True for SQLAlchemy internal dialects SQLite, MySQL/MariaDB, PostgreSQL,
    SQL Server.   It remains at False for Oracle, which provides native
    "executemany with RETURNING" support and also does not support
    ``supports_multivalues_insert``.    For MySQL/MariaDB, those MySQL
    dialects that don't support RETURNING will not report
    ``insert_executemany_returning`` as True.
    """

    use_insertmanyvalues_wo_returning: bool = unset
    """if True, and use_insertmanyvalues is also True, INSERT statements
    that don't include RETURNING will also use "insertmanyvalues".
    """

    insertmanyvalues_implicit_sentinel: InsertmanyvaluesSentinelOpts = unset
    """Options indicating the database supports a form of bulk INSERT where
    the autoincrement integer primary key can be reliably used as an ordering
    for INSERTed rows.
    """

    insertmanyvalues_page_size: int = unset
    """Number of rows to render into an individual INSERT..VALUES() statement
    for :attr:`.ExecuteStyle.INSERTMANYVALUES` executions.

    The default dialect defaults this to 1000.
    """

    insertmanyvalues_max_parameters: int = unset
    """Alternate to insertmanyvalues_page_size, will additionally limit
    page size based on number of parameters total in the statement.
    """

    supports_server_side_cursors: bool = unset
    """indicates if the dialect supports server side cursors"""

    server_side_cursors: bool = unset
    """deprecated; indicates if the dialect should attempt to use server
    side cursors by default"""

    server_version_info: tuple[Any, ...] | None = unset
    """a tuple containing a version number for the DB backend in use.

    This value is only available for supporting dialects, and is
    typically populated during the initial connection to the database.
    """

    default_schema_name: str | None = unset
    """the name of the default schema.  This value is only available for
    supporting dialects, and is typically populated during the
    initial connection to the database.
    """
    requires_name_normalize: bool = unset
    """
    indicates symbol names are
    UPPERCASEd if they are case insensitive
    within the database.
    if this is True, the methods normalize_name()
    and denormalize_name() must be provided.
    """

    is_async: bool = unset
    """Whether or not this dialect is intended for asyncio use."""

    has_terminate: bool = unset
    """Whether or not this dialect has a separate "terminate" implementation
    that does not block or require awaiting."""

    def __post_init__(self) -> None:
        if self.inherit is not None:
            ignore = {"inherit", "name", "driver"}
            for field in fields(self):
                if field.name in ignore:
                    continue

                self_value = getattr(self, field.name)
                if self_value is not unset:
                    continue

                inherit_value = getattr(self.inherit, field.name, MISSING)
                if inherit_value is not MISSING:
                    object.__setattr__(self, field.name, inherit_value)


class JDBCDialectMeta(type):
    _jdbc_wrapper_dialect_settings: DialectSettings

    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        /,
        **kwargs: Any,
    ) -> Any:
        settings: DialectSettings = namespace.pop("settings")
        as_dict_settings = asdict(settings)
        as_dict_settings.pop("inherit", None)
        namespace.update(
            (key, value)
            for key, value in as_dict_settings.items()
            if value is not unset
        )
        namespace["_jdbc_wrapper_dialect_settings"] = settings
        return super().__new__(cls, name, bases, namespace, **kwargs)


class JDBCDialectBase(DefaultDialect, metaclass=JDBCDialectMeta):
    _jdbc_wrapper_dialect_settings: DialectSettings
    settings = DialectSettings(
        name="jdbc_wrapper_base_dialect", driver="jdbc_wrapper_base_driver"
    )

    @classmethod
    @override
    def import_dbapi(cls) -> ModuleType:
        import jdbc_wrapper

        return DbapiModule(jdbc_wrapper)

    @property
    def dbapi(self) -> ModuleType:
        return self.import_dbapi()

    @dbapi.setter
    def dbapi(self, value: Any) -> None: ...

    @classmethod
    @override
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        if cls.is_async:
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

    def _create_connect_args(
        self, dsn: str, query: Mapping[str, Any]
    ) -> dict[str, Any]:
        driver_args = dict(query)
        try:
            driver: str = driver_args.pop("jdbc_driver")
        except KeyError as exc:
            raise OperationalError(
                "The `jdbc_driver` key is required in the query string"
            ) from exc

        result: dict[str, Any] = {
            "dsn": dsn,
            "driver": driver,
            "is_async": self.is_async,
        }

        modules = driver_args.pop("jdbc_modules", None)
        result["driver_args"] = driver_args
        if modules:
            if isinstance(modules, str):
                modules = (modules,)
            result["modules"] = modules
        return result

    ### settings:: start
    name: str
    """identifying name for the dialect from a DBAPI-neutral point of view
    (i.e. 'sqlite')
    """

    driver: str
    """identifying name for the dialect's DBAPI"""

    supports_alter: bool
    """``True`` if the database supports ``ALTER TABLE`` - used only for
    generating foreign key constraints in certain circumstances
    """

    supports_comments: bool
    """Indicates the dialect supports comment DDL on tables and columns."""

    supports_constraint_comments: bool
    """Indicates if the dialect supports comment DDL on constraints."""

    inline_comments: bool
    """Indicates the dialect supports comment DDL that's inline with the
    definition of a Table or Column.  If False, this implies that ALTER must
    be used to set table and column comments."""

    supports_statement_cache: bool
    """indicates if this dialect supports caching.

    All dialects that are compatible with statement caching should set this
    flag to True directly on each dialect class and subclass that supports
    it.  SQLAlchemy tests that this flag is locally present on each dialect
    subclass before it will use statement caching.  This is to provide
    safety for legacy or new dialects that are not yet fully tested to be
    compliant with SQL statement caching.
    """

    div_is_floordiv: bool
    """target database treats the / division operator as "floor division" """

    bind_typing: BindTyping
    """define a means of passing typing information to the database and/or
    driver for bound parameters.
    """

    include_set_input_sizes: set[Any] | None
    """set of DBAPI type objects that should be included in
    automatic cursor.setinputsizes() calls.
    """
    exclude_set_input_sizes: set[Any] | None
    """set of DBAPI type objects that should be excluded in
    automatic cursor.setinputsizes() calls.
    """

    default_sequence_base: int
    """the default value that will be rendered as the "START WITH" portion of
    a CREATE SEQUENCE DDL statement.
    """

    execute_sequence_format: type[tuple[Any, ...] | tuple[list[Any], ...]]
    """either the 'tuple' or 'list' type, depending on what cursor.execute()
    accepts for the second argument (they vary)."""

    supports_sequences: bool
    """Indicates if the dialect supports CREATE SEQUENCE or similar."""

    sequences_optional: bool
    """If True, indicates if the :paramref:`_schema.Sequence.optional`
    parameter on the :class:`_schema.Sequence` construct
    should signal to not generate a CREATE SEQUENCE. Applies only to
    dialects that support sequences. Currently used only to allow PostgreSQL
    SERIAL to be used on a column that specifies Sequence() for usage on
    other backends.
    """

    preexecute_autoincrement_sequences: bool
    """True if 'implicit' primary key functions must be executed separately
    in order to get their value, if RETURNING is not used.

    This is currently oriented towards PostgreSQL when the
    ``implicit_returning=False`` parameter is used on a :class:`.Table`
    object.
    """

    supports_identity_columns: bool
    """target database supports IDENTITY"""

    favor_returning_over_lastrowid: bool
    """for backends that support both a lastrowid and a RETURNING insert
    strategy, favor RETURNING for simple single-int pk inserts.

    cursor.lastrowid tends to be more performant on most backends.
    """

    update_returning: bool
    """if the dialect supports RETURNING with UPDATE"""

    delete_returning: bool
    """if the dialect supports RETURNING with DELETE"""

    update_returning_multifrom: bool
    """if the dialect supports RETURNING with UPDATE..FROM"""

    delete_returning_multifrom: bool
    """if the dialect supports RETURNING with DELETE..FROM"""

    insert_returning: bool
    """if the dialect supports RETURNING with INSERT"""

    cte_follows_insert: bool
    """target database, when given a CTE with an INSERT statement, needs
    the CTE to be below the INSERT"""

    supports_native_enum: bool
    """Indicates if the dialect supports a native ENUM construct.
    This will prevent :class:`_types.Enum` from generating a CHECK
    constraint when that type is used in "native" mode.
    """

    supports_native_boolean: bool
    """Indicates if the dialect supports a native boolean construct.
    This will prevent :class:`_types.Boolean` from generating a CHECK
    constraint when that type is used.
    """

    supports_native_uuid: bool
    """indicates if Python UUID() objects are handled natively by the
    driver for SQL UUID datatypes.
    """

    returns_native_bytes: bool
    """indicates if Python bytes() objects are returned natively by the
    driver for SQL "binary" datatypes.
    """

    supports_simple_order_by_label: bool
    """target database supports ORDER BY <labelname>, where <labelname>
    refers to a label in the columns clause of the SELECT"""

    tuple_in_values: bool
    """target database supports tuple IN, i.e. (x, y) IN ((q, p), (r, z))"""

    engine_config_types: Mapping[str, Any]
    """a mapping of string keys that can be in an engine config linked to
    type conversion functions.
    """

    supports_native_decimal: bool
    """indicates if Decimal objects are handled and returned for precision
    numeric types, or if floats are returned"""

    max_identifier_length: int
    """The maximum length of identifier names."""

    supports_sane_rowcount: bool
    """Indicate whether the dialect properly implements rowcount for
    ``UPDATE`` and ``DELETE`` statements.
    """

    supports_sane_multi_rowcount: bool
    """Indicate whether the dialect properly implements rowcount for
    ``UPDATE`` and ``DELETE`` statements when executed via
    executemany.
    """

    colspecs: MutableMapping[type[TypeEngine[Any]], type[TypeEngine[Any]]]
    """A dictionary of TypeEngine classes from sqlalchemy.types mapped
    to subclasses that are specific to the dialect class.  This
    dictionary is class-level only and is not accessed from the
    dialect instance itself.
    """

    supports_default_values: bool
    """dialect supports INSERT... DEFAULT VALUES syntax"""

    supports_default_metavalue: bool
    """dialect supports INSERT...(col) VALUES (DEFAULT) syntax."""

    default_metavalue_token: str
    """for INSERT... VALUES (DEFAULT) syntax, the token to put in the
    parenthesis.

    E.g. for SQLite this is the keyword "NULL".
    """

    supports_empty_insert: bool
    """dialect supports INSERT () VALUES (), i.e. a plain INSERT with no
    columns in it.

    This is not usually supported; an "empty" insert is typically
    suited using either "INSERT..DEFAULT VALUES" or
    "INSERT ... (col) VALUES (DEFAULT)".
    """

    supports_multivalues_insert: bool
    """Target database supports INSERT...VALUES with multiple value
    sets, i.e. INSERT INTO table (cols) VALUES (...), (...), (...), ...
    """

    use_insertmanyvalues: bool
    """if True, indicates "insertmanyvalues" functionality should be used
    to allow for ``insert_executemany_returning`` behavior, if possible.

    In practice, setting this to True means:

    if ``supports_multivalues_insert``, ``insert_returning`` and
    ``use_insertmanyvalues`` are all True, the SQL compiler will produce
    an INSERT that will be interpreted by the :class:`.DefaultDialect`
    as an :attr:`.ExecuteStyle.INSERTMANYVALUES` execution that allows
    for INSERT of many rows with RETURNING by rewriting a single-row
    INSERT statement to have multiple VALUES clauses, also executing
    the statement multiple times for a series of batches when large numbers
    of rows are given.

    The parameter is False for the default dialect, and is set to
    True for SQLAlchemy internal dialects SQLite, MySQL/MariaDB, PostgreSQL,
    SQL Server.   It remains at False for Oracle, which provides native
    "executemany with RETURNING" support and also does not support
    ``supports_multivalues_insert``.    For MySQL/MariaDB, those MySQL
    dialects that don't support RETURNING will not report
    ``insert_executemany_returning`` as True.
    """

    use_insertmanyvalues_wo_returning: bool
    """if True, and use_insertmanyvalues is also True, INSERT statements
    that don't include RETURNING will also use "insertmanyvalues".
    """

    insertmanyvalues_implicit_sentinel: InsertmanyvaluesSentinelOpts
    """Options indicating the database supports a form of bulk INSERT where
    the autoincrement integer primary key can be reliably used as an ordering
    for INSERTed rows.
    """

    insertmanyvalues_page_size: int
    """Number of rows to render into an individual INSERT..VALUES() statement
    for :attr:`.ExecuteStyle.INSERTMANYVALUES` executions.

    The default dialect defaults this to 1000.
    """

    insertmanyvalues_max_parameters: int
    """Alternate to insertmanyvalues_page_size, will additionally limit
    page size based on number of parameters total in the statement.
    """

    supports_server_side_cursors: bool
    """indicates if the dialect supports server side cursors"""

    server_side_cursors: bool
    """deprecated; indicates if the dialect should attempt to use server
    side cursors by default"""

    server_version_info: tuple[Any, ...] | None
    """a tuple containing a version number for the DB backend in use.

    This value is only available for supporting dialects, and is
    typically populated during the initial connection to the database.
    """

    default_schema_name: str | None
    """the name of the default schema.  This value is only available for
    supporting dialects, and is typically populated during the
    initial connection to the database.
    """
    requires_name_normalize: bool
    """
    indicates symbol names are
    UPPERCASEd if they are case insensitive
    within the database.
    if this is True, the methods normalize_name()
    and denormalize_name() must be provided.
    """

    is_async: bool
    """Whether or not this dialect is intended for asyncio use."""

    has_terminate: bool
    """Whether or not this dialect has a separate "terminate" implementation
    that does not block or require awaiting."""
    ### settings:: end


def wrap_async(
    func: Callable[_P, Awaitable[_T]] | Callable[_P, Coroutine[Any, Any, _T]],
) -> Callable[_P, _T]:
    @wraps(func)
    def inner(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        current = greenlet.getcurrent()
        if not check_async_greenlet(current) and not check_in_sa_greenlet(current):
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(_run, func, *args, **kwargs)
                wait([future], return_when="ALL_COMPLETED")
                return future.result()

        coro = func(*args, **kwargs)
        return await_(coro)

    return inner


async def _await(
    func: Callable[_P, Awaitable[_T]] | Callable[_P, Coroutine[Any, Any, _T]],
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    return await func(*args, **kwargs)


def _run(
    func: Callable[_P, Awaitable[_T]] | Callable[_P, Coroutine[Any, Any, _T]],
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    return asyncio.run(_await(func, *args, **kwargs))


class AsyncConnection(AdaptedConnection, ConnectionABC[Any]):
    __slots__ = ("_conection",)

    def __init__(self, connection: AsyncConnectionABC[Any]) -> None:
        self._conection = connection

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conection, name)

    @property
    @override
    def autocommit(self) -> bool:
        return self._connection.autocommit

    @autocommit.setter
    @override
    def autocommit(self, value: bool) -> None:
        self._conection.autocommit = value

    @property
    @override
    def is_closed(self) -> bool:
        return self._conection.is_closed

    @wrap_async
    @override
    async def close(self) -> None:
        return await self._conection.close()

    @wrap_async
    @override
    async def commit(self) -> None:
        return await self._conection.commit()

    @wrap_async
    @override
    async def rollback(self) -> None:
        return await self._conection.rollback()

    @wrap_async
    @override
    async def cursor(self) -> AsyncCursor:
        cursor = await self._conection.cursor()
        return AsyncCursor(self, cursor)

    @override
    def __enter__(self) -> Self:
        return self

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @override
    def __del__(self) -> None:
        with suppress(Exception):
            self.close()


class AsyncCursor(CursorABC[Any]):
    __slots__ = ("_connection", "_cursor")

    def __init__(
        self, connection: AsyncConnection, cursor: AsyncCursorABC[Any]
    ) -> None:
        self._connection = connection
        self._cursor = cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    @property
    @override
    def description(self) -> Sequence[Description] | None:
        return self._cursor.description

    @property
    @override
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @wrap_async
    @override
    async def callproc(
        self, procname: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> None:
        return await self._cursor.callproc(procname, parameters)

    @wrap_async
    @override
    async def close(self) -> None:
        return await self._cursor.close()

    @wrap_async
    @override
    async def _execute(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        await self._cursor.execute(operation, parameters)

    @wrap_async
    @override
    async def _executemany(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> None:
        await self._cursor.executemany(operation, seq_of_parameters)

    @wrap_async
    @override
    async def fetchone(self) -> Any | None:
        return await self._cursor.fetchone()

    @wrap_async
    @override
    async def fetchmany(self, size: int = -1) -> Sequence[Any]:
        return await self._cursor.fetchmany()

    @wrap_async
    @override
    async def fetchall(self) -> Sequence[Any]:
        return await self._cursor.fetchall()

    @wrap_async
    @override
    async def nextset(self) -> bool:
        return await self._cursor.nextset()

    @property
    @override
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    @override
    def arraysize(self, size: int) -> None:
        self._cursor.arraysize = size

    @wrap_async
    @override
    async def setinputsizes(self, sizes: Sequence[Any]) -> None:
        return await self._cursor.setinputsizes(sizes)

    @wrap_async
    @override
    async def setoutputsize(self, size: int, column: int = -1) -> None:
        return await self._cursor.setoutputsize(size, column)

    @property
    @override
    def rownumber(self) -> int:
        return self._cursor.rownumber

    @property
    @override
    def connection(self) -> ConnectionABC[Any]:
        return self._connection

    @wrap_async
    @override
    async def scroll(
        self, value: int, mode: Literal["relative", "absolute"] = "relative"
    ) -> None:
        return await self._cursor.scroll(value, mode)

    @wrap_async
    @override
    async def next(self) -> Any:
        return await self._cursor.next()

    @override
    def __iter__(self) -> Iterator[Any]:
        raise NotImplementedError

    @property
    @override
    def lastrowid(self) -> Any:
        return self._cursor.lastrowid

    @property
    @override
    def is_closed(self) -> bool:
        return self._cursor.is_closed

    @property
    @override
    def thread_id(self) -> int:
        return self._cursor.thread_id

    @thread_id.setter
    @override
    def thread_id(self, value: int) -> None:
        self._cursor.thread_id = value

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        with suppress(Exception):
            self._cursor._sync_cursor.close()  # type: ignore # noqa: SLF001


class DbapiModule(ModuleType):
    def __init__(self, jdbc_wrapper_module: ModuleType) -> None:
        self._module = jdbc_wrapper_module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._module, name)

    def __dir__(self) -> list[str]:
        return dir(self._module)

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        is_async = kwargs.pop("is_async", False)
        if is_async:
            # TODO, FIXME:
            # use `sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection`
            # use `sqlalchemy.connectors.asyncio.AsyncAdaptFallback_dbapi_connection`
            async_fallback = kwargs.pop("async_fallback", False)  # type: ignore # noqa: F841
            creator_fn = kwargs.pop("async_creator_fn", async_connect)  # type: ignore # noqa: F841
            connection = async_connect(*args, **kwargs)
            return AsyncConnection(connection)
        return sync_connect(*args, **kwargs)
