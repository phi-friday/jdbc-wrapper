from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import registry
from sqlalchemy.engine.url import URL, make_url

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Mapping

    from jdbc_wrapper._sqlalchemy.connector import JDBCConnector


def iter_jdbc_connector() -> Generator[type[JDBCConnector], None, None]:
    if "sqlite.jdbc_wrapper" not in registry.impls:
        from jdbc_wrapper._sqlalchemy import register_in_sqlalchemy

        register_in_sqlalchemy()

    for key in registry.impls:
        if key.endswith((".jdbc_wrapper", ".jdbc_async_wrapper")):
            yield registry.load(key)


def dsn_to_url(
    dsn: str, connector: type[JDBCConnector] | JDBCConnector | None = None
) -> URL:
    if connector is None:
        for jdbc_connector in iter_jdbc_connector():
            if dsn.startswith(jdbc_connector.jdbc_dsn_prefix[0]):
                connector = jdbc_connector
                break
        else:
            error_msg = f"Unknown JDBC DSN: {dsn}"
            raise ValueError(error_msg)

    dsn_prefix = "".join(connector.jdbc_dsn_prefix)
    url_parts = dsn.removeprefix(dsn_prefix)

    url = connector.name + "+" + connector.driver + "://" + url_parts
    return make_url(url)


def url_to_dsn(
    url: str | URL, connector: type[JDBCConnector] | JDBCConnector | None = None
) -> tuple[str, Mapping[str, Any]]:
    url = make_url(url)
    if connector is None:
        backend, driver = url.get_backend_name(), url.get_driver_name()
        for jdbc_connector in iter_jdbc_connector():
            if jdbc_connector.name == backend and jdbc_connector.driver == driver:
                connector = jdbc_connector
                break
        else:
            error_msg = f"Unknown JDBC URL: {url}"
            raise ValueError(error_msg)

    return connector.parse_dsn_parts(url)


def resolve_dsn_convertor(
    parts: str, convertor: Mapping[str | re.Pattern[str], str] | Callable[[str], str]
) -> str:
    if callable(convertor):
        return convertor(parts)

    for pattern, replacement in convertor.items():
        parts = re.sub(pattern, replacement, parts)

    return parts
