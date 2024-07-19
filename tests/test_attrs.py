# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

import threading
from datetime import datetime as datetime_class
from datetime import timezone
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

import jdbc_wrapper


@pytest.fixture(scope="module", autouse=True)
def records(sync_engine, model):
    records = [
        model(
            name="test",
            float=x,
            decimal=Decimal(f"{x}.{x}"),
            datetime=datetime_class.now(timezone.utc),
            boolean=True,
        )
        for x in range(10)
    ]
    with Session(sync_engine) as session:
        session.add_all(records)
        session.commit()
        records = (
            session.execute(
                sa.select(model).where(model.id.in_([x.id for x in records]))
            )
            .scalars()
            .all()
        )

    for record in records:
        assert record.id is not None
    return records


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
    insert_stmt = sa.insert(model).values([
        {
            "name": x.name,
            "float": x.float + 1,
            "decimal": x.decimal,
            "datetime": x.datetime,
            "boolean": x.boolean,
        }
        for x in records
    ])
    sync_cursor.execute(insert_stmt)
    assert sync_cursor.rowcount == size


def test_description(sync_cursor, model):
    assert sync_cursor.description is None
    select_stmt = sa.select(model).limit(1)
    sync_cursor.execute(select_stmt)
    assert sync_cursor.description is not None
    names = {x[0] for x in sync_cursor.description}
    assert names == set(model.__table__.columns.keys())


@pytest.mark.anyio()
async def test_autocommit_async(async_raw_connection):
    assert async_raw_connection.autocommit is False


@pytest.mark.anyio()
async def test_is_closed_async(async_raw_connection):
    assert async_raw_connection.is_closed is False
    await async_raw_connection.close()
    assert async_raw_connection.is_closed is True


@pytest.mark.anyio()
async def test_connection_async(async_cursor):
    assert isinstance(async_cursor.connection, jdbc_wrapper.AsyncConnection)


@pytest.mark.anyio()
async def test_thread_id_async(async_cursor):
    thread_id = threading.get_ident()
    assert async_cursor.thread_id == thread_id


@pytest.mark.anyio()
async def test_arraysize_async(async_cursor):
    size = 10
    assert async_cursor.arraysize == 1
    async_cursor.arraysize = size
    assert async_cursor.arraysize == size


@pytest.mark.anyio()
async def test_rowcount_async(async_cursor, model, records):
    size = len(records)
    assert async_cursor.rowcount == -1
    insert_stmt = sa.insert(model).values([
        {
            "name": x.name,
            "float": x.float + 1,
            "decimal": x.decimal,
            "datetime": x.datetime,
            "boolean": x.boolean,
        }
        for x in records
    ])
    await async_cursor.execute(insert_stmt)
    assert async_cursor.rowcount == size


@pytest.mark.anyio()
async def test_description_async(async_cursor, model):
    assert async_cursor.description is None
    select_stmt = sa.select(model).limit(1)
    await async_cursor.execute(select_stmt)
    assert async_cursor.description is not None
    names = {x[0] for x in async_cursor.description}
    assert names == set(model.__table__.columns.keys())


# TODO: def test_lastrowid(sync_cursor, model): ...
# TODO: async def test_lastrowid_async(async_cursor, model): ...
