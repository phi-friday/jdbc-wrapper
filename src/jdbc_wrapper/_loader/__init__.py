"""This module provides the Loader class for downloading jdbc drivers for each database.

However, it does not take responsibility for any bugs or issues
that may arise from its usage.
"""

from __future__ import annotations

from jdbc_wrapper._loader.mssql import MssqlLoader
from jdbc_wrapper._loader.postgresql import PostgresqlLoader
from jdbc_wrapper._loader.sqlite import SQliteLoader

__all__ = ["SQliteLoader", "PostgresqlLoader", "MssqlLoader"]
