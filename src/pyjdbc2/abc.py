"""PEP 249 -- Python Database API Specification v2.0

see more:
    https://peps.python.org/pep-0249/#connection-methods
    https://peps.python.org/pep-0249/#cursor-objects
"""

# pyright: reportIncompatibleMethodOverride=false
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Generic, Literal, NoReturn, overload

from typing_extensions import Self, TypeVar, override

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from types import TracebackType

    from pyjdbc2.types import Query

__all__ = []

_T_co = TypeVar("_T_co", covariant=True, bound="CursorABC")
_AT = TypeVar("_AT", bound="AsyncCursorABC")
_R_co = TypeVar("_R_co", covariant=True, bound=tuple[Any, ...], default=tuple[Any, ...])
_R2 = TypeVar("_R2", bound=tuple[Any, ...])


class ConnectionABC(ABC, Generic[_T_co]):
    @abstractmethod
    def close(self) -> None:
        """Close the connection now (rather than whenever `.__del__()` is called).

        The connection will be unusable from this point forward;
        an `Error` (or subclass) exception will be raised
        if any operation is attempted with the connection.

        The same applies to all cursor objects trying to use the connection.
        Note that closing a connection without committing the changes first
        will cause an implicit rollback to be performed.
        """

    @abstractmethod
    def commit(self) -> None:
        """Commit any pending transaction to the database.

        Note that if the database supports an auto-commit feature,
        this must be initially off.
        An interface method may be provided to turn it back on.

        Database modules that do not support transactions
        should implement this method with void functionality.
        """

    @abstractmethod
    def rollback(self) -> None:
        """This method is optional since not all databases provide transaction support.

        In case a database does provide transactions this method causes
        the database to roll back to the start of any pending transaction.

        Closing a connection without committing the changes first
        will cause an implicit rollback to be performed.
        """

    @abstractmethod
    def cursor(self) -> _T_co:
        """Return a new Cursor Object using the connection.

        If the database does not provide a direct cursor concept,
        the module will have to emulate cursors using other means
        to the extent needed by this specification.
        """

    @property
    @abstractmethod
    def autocommit(self) -> bool: ...

    @autocommit.setter
    @abstractmethod
    def autocommit(self, value: bool) -> None: ...

    @property
    @abstractmethod
    def is_closed(self) -> bool:
        """This read-only attribute returns `True` if the connection is closed,
        `False` otherwise.

        This attribute is useful to avoid exceptions when calling methods
        on a closed connection.
        """  # noqa: D205

    @abstractmethod
    def __enter__(self) -> Self: ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    @abstractmethod
    def __del__(self) -> None: ...


class AsyncConnectionABC(ConnectionABC[_AT], Generic[_AT]):
    @abstractmethod
    @override
    async def close(self) -> None: ...

    @abstractmethod
    @override
    async def commit(self) -> None: ...

    @abstractmethod
    @override
    async def rollback(self) -> None: ...

    @override
    def __enter__(self) -> Self:
        raise NotImplementedError("use aenter instead")

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        raise NotImplementedError("use aexit instead")

    @abstractmethod
    async def __aenter__(self) -> Self: ...

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class CursorABC(ABC, Generic[_R_co]):
    @property
    @abstractmethod
    def description(
        self,
    ) -> (
        Sequence[
            tuple[
                str,
                int | None,
                int | None,
                int | None,
                int | None,
                int | None,
                bool | None,
            ]
        ]
        | None
    ):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        * `name`
        * `type_code`
        * `display_size`
        * `internal_size`
        * `precision`
        * `scale`
        * `null_ok`

        The first two items (`name` and `type_code`) are mandatory,
        the other five are optional and are set to `None`
        if no meaningful values can be provided.

        This attribute will be `None` for operations that do not return rows or
        if the cursor has not had an operation invoked via the `.execute*()` method yet.

        The `type_code` can be interpreted by comparing it
        to the `Type Objects` specified in the section below.
        """

    @property
    @abstractmethod
    def rowcount(self) -> int:
        """This read-only attribute specifies the number of rows
        that the last `.execute*()` produced (for DQL statements like `SELECT`)
        or affected (for DML statements like `UPDATE` or `INSERT`).

        The attribute is -1 in case no `.execute*()` has been performed on the cursor
        or the rowcount of the last operation is cannot be determined by the interface.
        """  # noqa: D205

    @abstractmethod
    def callproc(
        self, procname: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> None:
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each argument
        that the procedure expects.
        The result of the call is returned as modified copy of the input sequence.

        Input parameters are left untouched,
        output and input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output.
        This must then be made available through the standard `.fetch*()` methods.
        """

    @abstractmethod
    def close(self) -> None:
        """Close the cursor now (rather than whenever `__del__` is called).

        The cursor will be unusable from this point forward;
        an `Error` (or subclass) exception will be raised
        if any operation is attempted with the cursor.
        """

    @overload
    def execute(
        self,
        operation: Query[_R2],
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> CursorABC[_R2]: ...
    @overload
    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> CursorABC[Any]: ...
    @overload
    def execute(
        self,
        operation: Query[_R2] | str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> CursorABC[_R2] | CursorABC[Any]: ...
    def execute(
        self,
        operation: Query[_R2] | str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> CursorABC[_R2] | CursorABC[Any]:
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping
        and will be bound to variables in the operation.
        Variables are specified in a database-specific notation
        (see the module’s paramstyle attribute for details).

        A reference to the operation will be retained by the cursor.
        If the same operation object is passed in again,
        then the cursor can optimize its behavior.
        This is most effective for algorithms where the same operation is used,
        but different parameters are bound to it (many times).

        For maximum efficiency when reusing an operation,
        it is best to use the `.setinputsizes()` method to specify the parameter types
        and sizes ahead of time.
        It is legal for a parameter to not match the predefined information;
        the implementation should compensate, possibly with a loss of efficiency.

        The parameters may also be specified as list of tuples to e.g.
        insert multiple rows in a single operation,
        but this kind of usage is deprecated: `.executemany()` should be used instead.
        """  # noqa: RUF002
        return (
            self._execute(operation, parameters)
            if isinstance(operation, str)
            else self._execute(operation.statement, parameters)
        )

    @abstractmethod
    def _execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> CursorABC[Any]: ...

    @overload
    def executemany(
        self,
        operation: Query[_R2],
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[_R2]: ...
    @overload
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[Any]: ...
    @overload
    def executemany(
        self,
        operation: Query[_R2] | str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[_R2] | CursorABC[Any]: ...

    def executemany(
        self,
        operation: Query[_R2] | str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[_R2] | CursorABC[Any]:
        """Prepare a database operation (query or command)
        and then execute it against all parameter sequences or mappings found
        in the sequence seq_of_parameters.

        Modules are free to implement this method using multiple calls
        to the `.execute()` method or by using array operations
        to have the database process the sequence as a whole in one call.

        Use of this method for an operation
        which produces one or more result sets constitutes undefined behavior,
        and the implementation is permitted (but not required)
        to raise an exception when it detects
        that a result set has been created by an invocation of the operation.

        The same comments as for `.execute()` also apply accordingly to this method.
        """  # noqa: D205
        return (
            self._executemany(operation, seq_of_parameters)
            if isinstance(operation, str)
            else self._executemany(operation.statement, seq_of_parameters)
        )

    @abstractmethod
    def _executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[Any]: ...

    @abstractmethod
    def fetchone(self) -> _R_co | None:
        """Fetch the next row of a query result set,
        returning a single sequence, or None when no more data is available.

        An `Error` (or subclass) exception is raised
        if the previous call to `.execute*()` did not produce any result set
        or no call was issued yet.
        """  # noqa: D205

    @abstractmethod
    def fetchmany(self, size: int = -1) -> Sequence[_R_co]:
        """Fetch the next set of rows of a query result,
        returning a sequence of sequences (e.g. a list of tuples).
        An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter.
        If it is not given,
        the cursor’s arraysize determines the number of rows to be fetched.
        The method should try to fetch as many rows as indicated by the size parameter.
        If this is not possible due to the specified number of rows not being available,
        fewer rows may be returned.

        An `Error` (or subclass) exception is raised
        if the previous call to `.execute*()` did not produce any result set
        or no call was issued yet.

        Note there are performance considerations involved with the size parameter.
        For optimal performance, it is usually best to use the `.arraysize` attribute.
        If the size parameter is used, then it is best for it to retain the same value
        from one `.fetchmany()` call to the next.
        """  # noqa: D205, RUF002

    @abstractmethod
    def fetchall(self) -> Sequence[_R_co]:
        """Fetch all (remaining) rows of a query result,
        returning them as a sequence of sequences (e.g. a list of tuples).
        Note that the cursor’s arraysize attribute can affect
        the performance of this operation.

        An `Error` (or subclass) exception is raised
        if the previous call to `.execute*()` did not produce any result set
        or no call was issued yet.
        """  # noqa: D205, RUF002

    @abstractmethod
    def nextset(self) -> bool:
        """This method will make the cursor skip to the next available set,
        discarding any remaining rows from the current set.

        If there are no more sets, the method returns None.
        Otherwise, it returns a true value and subsequent calls
        to the `.fetch*()` methods will return rows from the next result set.

        An `Error` (or subclass) exception is raised
        if the previous call to `.execute*()` did not produce any result set
        or no call was issued yet.
        """  # noqa: D205

    @property
    @abstractmethod
    def arraysize(self) -> int:
        """This read/write attribute specifies the number of rows
        to fetch at a time with `.fetchmany()`.
        It defaults to 1 meaning to fetch a single row at a time.

        Implementations must observe this value
        with respect to the `.fetchmany()` method,
        but are free to interact with the database a single row at a time.
        It may also be used in the implementation of `.executemany()`.
        """  # noqa: D205

    @arraysize.setter
    @abstractmethod
    def arraysize(self, size: int) -> None: ...

    @abstractmethod
    def setinputsizes(self, sizes: Sequence[int]) -> None:
        """This can be used before a call to `.execute*()` to predefine memory areas
        for the operation’s parameters.

        sizes is specified as a sequence — one item for each input parameter.
        The item should be a Type Object that corresponds
        to the input that will be used,
        or it should be an integer specifying the maximum length of a string parameter.

        If the item is `None`,
        then no predefined memory area will be reserved for that column
        (this is useful to avoid predefined areas for large inputs).

        This method would be used before the `.execute*()` method is invoked.
        """  # noqa: D205, RUF002

    @abstractmethod
    def setoutputsize(self, size: int, column: int = -1) -> None:
        """Set a column buffer size for fetches of large columns
        (e.g. `LONG`s, `BLOB`s, etc.).

        The column is specified as an index into the result sequence.
        Not specifying the column will set the default size
        for all large columns in the cursor.

        This method would be used before the `.execute*()` method is invoked.
        """  # noqa: D205

    @property
    @abstractmethod
    def rownumber(self) -> int:
        """This read-only attribute should provide
        the current 0-based index of the cursor
        in the result set or None if the index cannot be determined.

        The index can be seen as index of the cursor in a sequence (the result set).
        The next fetch operation will fetch
        the row indexed by `.rownumber` in that sequence.
        """  # noqa: D205

    @property
    @abstractmethod
    def connection(self) -> ConnectionABC[Self]:
        """This read-only attribute return a reference to the `Connection` object
        on which the cursor was created.

        The attribute simplifies writing polymorph code
        in multi-connection environments.
        """  # noqa: D205

    @abstractmethod
    def scroll(
        self, value: int, mode: Literal["relative", "absolute"] = "relative"
    ) -> None:
        """Scroll the cursor in the result set to a new position according to mode.

        If mode is `relative` (default),
        value is taken as offset to the current position in the result set,
        if set to `absolute`, value states an absolute target position.

        An `IndexError` should be raised
        in case a scroll operation would leave the result set.
        In this case, the cursor position is left undefined
        (ideal would be to not move the cursor at all).
        """

    @abstractmethod
    def next(self) -> _R_co:
        """Return the next row from the currently executing SQL statement
        using the same semantics as `.fetchone()`.

        A `StopIteration` exception is raised
        when the result set is exhausted for Python versions 2.2 and later.
        Previous versions don’t have the `StopIteration` exception
        and so the method should raise an `IndexError` instead.
        """  # noqa: D205, RUF002

    @abstractmethod
    def __iter__(self) -> Iterator[_R_co]:
        """Return self to make cursors compatible to the iteration protocol"""

    @property
    @abstractmethod
    def lastrowid(self) -> Any:
        """This read-only attribute provides the rowid of the last modified row
        (most databases return a rowid only
        when a single `INSERT` operation is performed).

        If the operation does not set a rowid or
        if the database does not support rowids,
        this attribute should be set to `None`.

        The semantics of `.lastrowid` are undefined
        in case the last executed statement modified more than one row, e.g.
        when using `INSERT` with `.executemany()`.
        """  # noqa: D205

    @property
    @abstractmethod
    def is_closed(self) -> bool:
        """This read-only attribute returns `True` if the cursor is closed,
        `False` otherwise.

        This attribute is useful to avoid exceptions when calling methods
        on a closed cursor.
        """  # noqa: D205

    @abstractmethod
    def __enter__(self) -> Self: ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    @abstractmethod
    def __del__(self) -> None: ...


class AsyncCursorABC(CursorABC[_R_co], Generic[_R_co]):
    @abstractmethod
    @override
    async def callproc(
        self, procname: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> None: ...

    @abstractmethod
    @override
    async def close(self) -> None: ...

    @overload
    async def execute(
        self,
        operation: Query[_R2],
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncCursorABC[_R2]: ...
    @overload
    async def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncCursorABC[Any]: ...
    @overload
    async def execute(
        self,
        operation: Query[_R2] | str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncCursorABC[_R2] | AsyncCursorABC[Any]: ...
    @override
    async def execute(
        self,
        operation: Query[_R2] | str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncCursorABC[_R2] | AsyncCursorABC[Any]:
        return await (
            self._execute(operation, parameters)
            if isinstance(operation, str)
            else self._execute(operation.statement, parameters)
        )

    @abstractmethod
    @override
    async def _execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncCursorABC[Any]: ...

    @overload
    async def executemany(
        self,
        operation: Query[_R2],
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> AsyncCursorABC[_R2]: ...
    @overload
    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> AsyncCursorABC[Any]: ...
    @overload
    async def executemany(
        self,
        operation: Query[_R2] | str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> AsyncCursorABC[_R2] | AsyncCursorABC[Any]: ...
    @override
    async def executemany(
        self,
        operation: Query[_R2] | str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[_R2] | CursorABC[Any]:
        return await (
            self._executemany(operation, seq_of_parameters)
            if isinstance(operation, str)
            else self._executemany(operation.statement, seq_of_parameters)
        )

    @abstractmethod
    @override
    async def _executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    ) -> CursorABC[Any]: ...

    @abstractmethod
    @override
    async def fetchone(self) -> _R_co | None: ...

    @abstractmethod
    @override
    async def fetchmany(self, size: int = -1) -> Sequence[_R_co]: ...

    @abstractmethod
    @override
    async def fetchall(self) -> Sequence[_R_co]: ...

    @abstractmethod
    @override
    async def nextset(self) -> bool: ...

    @abstractmethod
    @override
    async def setinputsizes(self, sizes: Sequence[int]) -> None: ...

    @abstractmethod
    @override
    async def setoutputsize(self, size: int, column: int = -1) -> None: ...

    @property
    @abstractmethod
    @override
    def connection(self) -> AsyncConnectionABC[Self]: ...

    @abstractmethod
    @override
    async def scroll(
        self, value: int, mode: Literal["relative", "absolute"] = "relative"
    ) -> None: ...

    @abstractmethod
    @override
    async def next(self) -> _R_co: ...

    @override
    def __iter__(self) -> NoReturn:
        raise NotImplementedError

    @abstractmethod
    def __aiter__(self) -> Self:
        """Return self to make cursors compatible to the iteration protocol"""

    @override
    def __enter__(self) -> Self:
        raise NotImplementedError("use aenter instead")

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        raise NotImplementedError("use aexit instead")

    @abstractmethod
    async def __aenter__(self) -> Self: ...

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
