# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

import pytest

import jdbc_wrapper


def test_connect_sync(jdbc_dsn, jdbc_driver, jdbc_modules, jdbc_driver_args):
    connection = jdbc_wrapper.connect(
        jdbc_dsn, jdbc_driver, jdbc_modules, jdbc_driver_args, is_async=False
    )
    assert isinstance(connection, jdbc_wrapper.Connection)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("select 1")
            row = cursor.fetchone()

    assert row == (1,)


@pytest.mark.anyio()
async def test_connect_async(jdbc_dsn, jdbc_driver, jdbc_modules, jdbc_driver_args):
    connection = jdbc_wrapper.connect(
        jdbc_dsn, jdbc_driver, jdbc_modules, jdbc_driver_args, is_async=True
    )
    assert isinstance(connection, jdbc_wrapper.AsyncConnection)
    async with connection:
        async with connection.cursor() as cursor:
            await cursor.execute("select 1")
            row = await cursor.fetchone()

    assert row == (1,)
