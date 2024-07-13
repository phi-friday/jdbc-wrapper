from __future__ import annotations

import sys
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import util as sa_util
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.interfaces import BindTyping
from sqlalchemy.sql.compiler import InsertmanyvaluesSentinelOpts
from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping
    from types import ModuleType

    from sqlalchemy.types import TypeEngine

_dataclass_options = {"frozen": True}
if sys.version_info >= (3, 10):
    _dataclass_options["kw_only"] = True
    _dataclass_options["slots"] = False


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

    supports_alter: bool = True
    """``True`` if the database supports ``ALTER TABLE`` - used only for
    generating foreign key constraints in certain circumstances
    """

    supports_comments: bool = False
    """Indicates the dialect supports comment DDL on tables and columns."""

    supports_constraint_comments: bool = False
    """Indicates if the dialect supports comment DDL on constraints."""

    inline_comments: bool = False
    """Indicates the dialect supports comment DDL that's inline with the
    definition of a Table or Column.  If False, this implies that ALTER must
    be used to set table and column comments."""

    supports_statement_cache: bool = True
    """indicates if this dialect supports caching.

    All dialects that are compatible with statement caching should set this
    flag to True directly on each dialect class and subclass that supports
    it.  SQLAlchemy tests that this flag is locally present on each dialect
    subclass before it will use statement caching.  This is to provide
    safety for legacy or new dialects that are not yet fully tested to be
    compliant with SQL statement caching.
    """

    div_is_floordiv: bool = True
    """target database treats the / division operator as "floor division" """

    bind_typing: BindTyping = BindTyping.NONE
    """define a means of passing typing information to the database and/or
    driver for bound parameters.
    """

    include_set_input_sizes: set[Any] | None = None
    """set of DBAPI type objects that should be included in
    automatic cursor.setinputsizes() calls.
    """
    exclude_set_input_sizes: set[Any] | None = None
    """set of DBAPI type objects that should be excluded in
    automatic cursor.setinputsizes() calls.
    """

    default_sequence_base: int = 1
    """the default value that will be rendered as the "START WITH" portion of
    a CREATE SEQUENCE DDL statement.
    """

    execute_sequence_format: type[tuple[Any, ...] | tuple[list[Any], ...]] = tuple
    """either the 'tuple' or 'list' type, depending on what cursor.execute()
    accepts for the second argument (they vary)."""

    supports_sequences: bool = False
    """Indicates if the dialect supports CREATE SEQUENCE or similar."""

    sequences_optional: bool = False
    """If True, indicates if the :paramref:`_schema.Sequence.optional`
    parameter on the :class:`_schema.Sequence` construct
    should signal to not generate a CREATE SEQUENCE. Applies only to
    dialects that support sequences. Currently used only to allow PostgreSQL
    SERIAL to be used on a column that specifies Sequence() for usage on
    other backends.
    """

    preexecute_autoincrement_sequences: bool = False
    """True if 'implicit' primary key functions must be executed separately
    in order to get their value, if RETURNING is not used.

    This is currently oriented towards PostgreSQL when the
    ``implicit_returning=False`` parameter is used on a :class:`.Table`
    object.
    """

    supports_identity_columns: bool = False
    """target database supports IDENTITY"""

    favor_returning_over_lastrowid: bool = False
    """for backends that support both a lastrowid and a RETURNING insert
    strategy, favor RETURNING for simple single-int pk inserts.

    cursor.lastrowid tends to be more performant on most backends.
    """

    update_returning: bool = False
    """if the dialect supports RETURNING with UPDATE"""

    delete_returning: bool = False
    """if the dialect supports RETURNING with DELETE"""

    update_returning_multifrom: bool = False
    """if the dialect supports RETURNING with UPDATE..FROM"""

    delete_returning_multifrom: bool = False
    """if the dialect supports RETURNING with DELETE..FROM"""

    insert_returning: bool = False
    """if the dialect supports RETURNING with INSERT"""

    cte_follows_insert: bool = False
    """target database, when given a CTE with an INSERT statement, needs
    the CTE to be below the INSERT"""

    supports_native_enum: bool = False
    """Indicates if the dialect supports a native ENUM construct.
    This will prevent :class:`_types.Enum` from generating a CHECK
    constraint when that type is used in "native" mode.
    """

    supports_native_boolean: bool = False
    """Indicates if the dialect supports a native boolean construct.
    This will prevent :class:`_types.Boolean` from generating a CHECK
    constraint when that type is used.
    """

    supports_native_uuid: bool = False
    """indicates if Python UUID() objects are handled natively by the
    driver for SQL UUID datatypes.
    """

    returns_native_bytes: bool = False
    """indicates if Python bytes() objects are returned natively by the
    driver for SQL "binary" datatypes.
    """

    supports_simple_order_by_label: bool = True
    """target database supports ORDER BY <labelname>, where <labelname>
    refers to a label in the columns clause of the SELECT"""

    tuple_in_values: bool = False
    """target database supports tuple IN, i.e. (x, y) IN ((q, p), (r, z))"""

    engine_config_types: Mapping[str, Any] = field(
        default_factory=lambda: sa_util.immutabledict({
            "pool_timeout": sa_util.asint,
            "echo": sa_util.bool_or_str("debug"),
            "echo_pool": sa_util.bool_or_str("debug"),
            "pool_recycle": sa_util.asint,
            "pool_size": sa_util.asint,
            "max_overflow": sa_util.asint,
            "future": sa_util.asbool,
        })
    )
    """a mapping of string keys that can be in an engine config linked to
    type conversion functions.
    """

    supports_native_decimal: bool = False
    """indicates if Decimal objects are handled and returned for precision
    numeric types, or if floats are returned"""

    max_identifier_length: int = 9999
    """The maximum length of identifier names."""

    supports_sane_rowcount: bool = True
    """Indicate whether the dialect properly implements rowcount for
    ``UPDATE`` and ``DELETE`` statements.
    """

    supports_sane_multi_rowcount: bool = True
    """Indicate whether the dialect properly implements rowcount for
    ``UPDATE`` and ``DELETE`` statements when executed via
    executemany.
    """

    colspecs: MutableMapping[type[TypeEngine[Any]], type[TypeEngine[Any]]] = field(
        default_factory=dict
    )
    """A dictionary of TypeEngine classes from sqlalchemy.types mapped
    to subclasses that are specific to the dialect class.  This
    dictionary is class-level only and is not accessed from the
    dialect instance itself.
    """

    supports_default_values: bool = False
    """dialect supports INSERT... DEFAULT VALUES syntax"""

    supports_default_metavalue: bool = False
    """dialect supports INSERT...(col) VALUES (DEFAULT) syntax."""

    default_metavalue_token: str = "DEFAULT"
    """for INSERT... VALUES (DEFAULT) syntax, the token to put in the
    parenthesis.

    E.g. for SQLite this is the keyword "NULL".
    """

    supports_empty_insert: bool = True
    """dialect supports INSERT () VALUES (), i.e. a plain INSERT with no
    columns in it.

    This is not usually supported; an "empty" insert is typically
    suited using either "INSERT..DEFAULT VALUES" or
    "INSERT ... (col) VALUES (DEFAULT)".
    """

    supports_multivalues_insert: bool = False
    """Target database supports INSERT...VALUES with multiple value
    sets, i.e. INSERT INTO table (cols) VALUES (...), (...), (...), ...
    """

    use_insertmanyvalues: bool = False
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

    use_insertmanyvalues_wo_returning: bool = False
    """if True, and use_insertmanyvalues is also True, INSERT statements
    that don't include RETURNING will also use "insertmanyvalues".
    """

    insertmanyvalues_implicit_sentinel: InsertmanyvaluesSentinelOpts = (
        InsertmanyvaluesSentinelOpts.NOT_SUPPORTED
    )
    """Options indicating the database supports a form of bulk INSERT where
    the autoincrement integer primary key can be reliably used as an ordering
    for INSERTed rows.
    """

    insertmanyvalues_page_size: int = 1000
    """Number of rows to render into an individual INSERT..VALUES() statement
    for :attr:`.ExecuteStyle.INSERTMANYVALUES` executions.

    The default dialect defaults this to 1000.
    """

    insertmanyvalues_max_parameters: int = 32700
    """Alternate to insertmanyvalues_page_size, will additionally limit
    page size based on number of parameters total in the statement.
    """

    supports_server_side_cursors: bool = False
    """indicates if the dialect supports server side cursors"""

    server_side_cursors: bool = False
    """deprecated; indicates if the dialect should attempt to use server
    side cursors by default"""

    server_version_info: tuple[Any, ...] | None = None
    """a tuple containing a version number for the DB backend in use.

    This value is only available for supporting dialects, and is
    typically populated during the initial connection to the database.
    """

    default_schema_name: str | None = None
    """the name of the default schema.  This value is only available for
    supporting dialects, and is typically populated during the
    initial connection to the database.
    """
    requires_name_normalize: bool = False
    """
    indicates symbol names are
    UPPERCASEd if they are case insensitive
    within the database.
    if this is True, the methods normalize_name()
    and denormalize_name() must be provided.
    """

    is_async: bool = False
    """Whether or not this dialect is intended for asyncio use."""

    has_terminate: bool = False
    """Whether or not this dialect has a separate "terminate" implementation
    that does not block or require awaiting."""


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
        namespace.update(asdict(settings))
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

        return jdbc_wrapper

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
