from __future__ import annotations


def register_in_sqlalchemy() -> None:
    from sqlalchemy.dialects import registry

    registry.register(
        "jdbc_wrapper.postgresql",
        "jdbc_wrapper._sqlalchemy.postgresql",
        "PGJDBCDialect",
    )
    registry.register(
        "jdbc_wrapper.mssql", "jdbc_wrapper._sqlalchemy.mssql", "MSJDBCDialect"
    )
