from __future__ import annotations


def register_in_sqlalchemy() -> None:
    from sqlalchemy.dialects import registry

    registry.register(
        "jdbc_wrapper.pg", "jdbc_wrapper._sqlalchemy.pg_jdbc_wrapper", "PGJDBCDialect"
    )
