# ruff: noqa: D205
"""https://peps.python.org/pep-0249/#exceptions"""

from __future__ import annotations

from jpype import dbapi2 as jpype_dbapi2


class Error(jpype_dbapi2.Error, Exception):
    """Exception raised for important warnings
    like data truncations while inserting, etc.
    It must be a subclass of the Python `Exception` class
    """


class Warning(jpype_dbapi2.Warning, Exception):  # noqa: N818, A001
    """Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single `except` statement.
    Warnings are not considered errors and thus should not use this class as base.
    It must be a subclass of the Python `Exception` class
    """


class InterfaceError(jpype_dbapi2.IntegrityError, Error):
    """Exception raised for errors
    that are related to the database interface rather than the database itself.
    It must be a subclass of `Error`.
    """


class DatabaseError(jpype_dbapi2.DatabaseError, Error):
    """Exception raised for errors that are related to the database.
    It must be a subclass of `Error`.
    """


class DataError(jpype_dbapi2.DataError, DatabaseError):
    """Exception raised for errors that are due to problems with the processed data
    like division by zero, numeric value out of range, etc.
    It must be a subclass of `DatabaseError`.
    """


class OperationalError(jpype_dbapi2.OperationalError, DatabaseError):
    """Exception raised for errors
    that are related to the database’s operation and not necessarily
    under the control of the programmer,
    e.g. an unexpected disconnect occurs,
    the data source name is not found,
    a transaction could not be processed,
    a memory allocation error occurred during processing,
    etc. It must be a subclass of `DatabaseError`.
    """  # noqa: RUF002


class IntegrityError(jpype_dbapi2.IntegrityError, DatabaseError):
    """Exception raised when the relational integrity of the database is affected,
    e.g. a foreign key check fails.
    It must be a subclass of `DatabaseError`.
    """


class InternalError(jpype_dbapi2.InternalError, DatabaseError):
    """Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    It must be a subclass of `DatabaseError`.
    """


class ProgrammingError(jpype_dbapi2.ProgrammingError, DatabaseError):
    """Exception raised for programming errors,
    e.g. table not found or already exists,
    syntax error in the SQL statement,
    wrong number of parameters specified, etc.
    It must be a subclass of `DatabaseError`.
    """


class NotSupportedError(jpype_dbapi2.NotSupportedError, DatabaseError):
    """Exception raised in case a method or database API was used
    which is not supported by the database, e.g.
    requesting a `.rollback()` on a connection
    that does not support transaction or has transactions turned off.
    It must be a subclass of `DatabaseError`.
    """
