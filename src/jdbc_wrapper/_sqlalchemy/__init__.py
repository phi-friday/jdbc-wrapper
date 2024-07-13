from __future__ import annotations


def register_in_sqlalchemy() -> None:
    from sqlalchemy.dialects import registry

    registry.register(
        "postgresql.jdbc_wrapper",
        "jdbc_wrapper._sqlalchemy.postgresql",
        "PGJDBCDialect",
    )
    registry.register(
        "mssql.jdbc_wrapper", "jdbc_wrapper._sqlalchemy.mssql", "MSJDBCDialect"
    )
