# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

import threading
from functools import reduce

import pytest
import sqlalchemy as sa

import jdbc_wrapper

pytestmark = pytest.mark.anyio


def test_autocommit(sync_raw_connection):
    assert sync_raw_connection.autocommit is False


def test_is_closed(sync_raw_connection):
    assert sync_raw_connection.is_closed is False
    sync_raw_connection.close()
    assert sync_raw_connection.is_closed is True
    jdbc_connection_closed = sync_raw_connection._jpype_connection._jcx.isClosed()  # noqa: SLF001
    assert jdbc_connection_closed is True


def test_connection(sync_cursor):
    assert isinstance(sync_cursor.connection, jdbc_wrapper.Connection)


def test_thread_id(sync_cursor):
    thread_id = threading.get_ident()
    assert sync_cursor.thread_id == thread_id


def test_arraysize(sync_cursor):
    size = 10
    assert sync_cursor.arraysize == 1
    assert sync_cursor._jpype_cursor.arraysize == 1  # noqa: SLF001
    sync_cursor.arraysize = size
    assert sync_cursor.arraysize == size
    assert sync_cursor._jpype_cursor.arraysize == size  # noqa: SLF001


def test_rowcount(sync_cursor, model, records):
    size = len(records)
    assert sync_cursor.rowcount == -1

    params = reduce(
        lambda x, y: x | y,  # pyright: ignore[reportUnknownLambdaType]
        (
            {
                f"name_{idx}": x.name,
                f"float_{idx}": x.float + 1,
                f"decimal_{idx}": x.decimal,
                f"datetime_{idx}": x.datetime,
                f"date_{idx}": x.date,
                f"time_{idx}": x.time,
                f"boolean_{idx}": x.boolean,
                f"unique_{idx}": f"{x.unique}_{idx}",
            }
            for idx, x in enumerate(records)
        ),
    )
    insert_stmt = sa.insert(model).values([
        {
            "name": sa.bindparam(f"name_{idx}"),
            "float": sa.bindparam(f"float_{idx}"),
            "decimal": sa.bindparam(f"decimal_{idx}"),
            "datetime": sa.bindparam(f"datetime_{idx}"),
            "date": sa.bindparam(f"date_{idx}"),
            "time": sa.bindparam(f"time_{idx}"),
            "boolean": sa.bindparam(f"boolean_{idx}"),
            "unique": sa.bindparam(f"unique_{idx}"),
        }
        for idx in range(len(records))
    ])
    sync_cursor.execute(insert_stmt, params)
    assert sync_cursor.rowcount == size


def test_description(sync_cursor, model, table):
    assert sync_cursor.description is None
    select_stmt = sa.select(model).limit(sa.bindparam("size"))
    sync_cursor.execute(select_stmt, {"size": 1})
    assert sync_cursor.description is not None
    names = {x[0] for x in sync_cursor.description}
    assert names == set(table.columns.keys())


async def test_autocommit_async(async_raw_connection):
    assert async_raw_connection.autocommit is False


async def test_is_closed_async(async_raw_connection):
    assert async_raw_connection.is_closed is False
    await async_raw_connection.close()
    assert async_raw_connection.is_closed is True


async def test_connection_async(async_cursor):
    assert isinstance(async_cursor.connection, jdbc_wrapper.AsyncConnection)


async def test_thread_id_async(async_cursor):
    thread_id = threading.get_ident()
    assert async_cursor.thread_id == thread_id


async def test_arraysize_async(async_cursor):
    size = 10
    assert async_cursor.arraysize == 1
    async_cursor.arraysize = size
    assert async_cursor.arraysize == size


async def test_rowcount_async(async_cursor, model, records):
    size = len(records)
    assert async_cursor.rowcount == -1

    params = reduce(
        lambda x, y: x | y,  # pyright: ignore[reportUnknownLambdaType]
        (
            {
                f"name_{idx}": x.name,
                f"float_{idx}": x.float + 1,
                f"decimal_{idx}": x.decimal,
                f"datetime_{idx}": x.datetime,
                f"date_{idx}": x.date,
                f"time_{idx}": x.time,
                f"boolean_{idx}": x.boolean,
                f"unique_{idx}": f"{x.unique}_{idx}",
            }
            for idx, x in enumerate(records)
        ),
    )
    insert_stmt = sa.insert(model).values([
        {
            "name": sa.bindparam(f"name_{idx}"),
            "float": sa.bindparam(f"float_{idx}"),
            "decimal": sa.bindparam(f"decimal_{idx}"),
            "datetime": sa.bindparam(f"datetime_{idx}"),
            "date": sa.bindparam(f"date_{idx}"),
            "time": sa.bindparam(f"time_{idx}"),
            "boolean": sa.bindparam(f"boolean_{idx}"),
            "unique": sa.bindparam(f"unique_{idx}"),
        }
        for idx in range(len(records))
    ])
    await async_cursor.execute(insert_stmt, params)
    assert async_cursor.rowcount == size


async def test_description_async(async_cursor, model, table):
    assert async_cursor.description is None
    select_stmt = sa.select(model).limit(sa.bindparam("limit"))
    await async_cursor.execute(select_stmt, {"limit": 1})
    assert async_cursor.description is not None
    names = {x[0] for x in async_cursor.description}
    assert names == set(table.columns.keys())


# TODO: def test_lastrowid(sync_cursor, model): ...
# TODO: async def test_lastrowid_async(async_cursor, model): ...
