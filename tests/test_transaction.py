# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

from datetime import datetime as datetime_class
from datetime import timezone
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

pytestmark = pytest.mark.anyio


def test_rollback(sync_session, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc),
        boolean=True,
    )
    sync_session.add(new)
    sync_session.flush()
    sync_session.refresh(new)
    new_id = int(new.id)
    sync_session.rollback()
    maybe = sync_session.get(model, new_id)
    assert maybe is None


def test_no_commit(sync_engine, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc),
        boolean=True,
    )
    with Session(sync_engine) as session:
        session.add(new)
        session.flush()
        session.refresh(new)
        new_id = int(new.id)

    with Session(sync_engine) as session:
        maybe = session.get(model, new_id)
        assert maybe is None


def test_commit(sync_engine, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc),
        boolean=True,
    )
    with Session(sync_engine) as session:
        session.add(new)
        session.flush()
        session.commit()
        new_id = int(new.id)

    with Session(sync_engine) as session:
        maybe = session.get(model, new_id)
        assert maybe is not None

    assert new.id == maybe.id
    assert new.name == maybe.name
    assert new.float == maybe.float
    assert new.decimal == maybe.decimal
    assert new.datetime == maybe.datetime
    assert new.boolean is maybe.boolean


def test_autocommit(sync_raw_connection, sync_session, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc).replace(tzinfo=None),
        boolean=True,
    )

    param = {
        "name": new.name,
        "float": new.float,
        "decimal": new.decimal,
        "datetime": new.datetime,
        "boolean": new.boolean,
    }
    insert_stmt = sa.insert(model).values(
        name=sa.bindparam("name"),
        float=sa.bindparam("float"),
        decimal=sa.bindparam("decimal"),
        datetime=sa.bindparam("datetime"),
        boolean=sa.bindparam("boolean"),
    )
    select_stmt = (
        sa.select(model)
        .with_only_columns(
            model.name, model.float, model.decimal, model.datetime, model.boolean
        )
        .where(
            model.name == sa.bindparam("name", type_=sa.String()),
            model.float == sa.bindparam("float", type_=sa.Float()),
            model.decimal == sa.bindparam("decimal", type_=sa.Numeric()),
            model.datetime
            == sa.bindparam("datetime", type_=sa.DateTime(timezone=False)),
            model.boolean == sa.bindparam("boolean", type_=sa.Boolean()),
        )
        .order_by(model.datetime.desc())
    )

    sync_raw_connection.autocommit = True
    assert sync_raw_connection.autocommit is True
    with sync_raw_connection.cursor() as cursor:
        cursor.execute(insert_stmt, param)
        cursor.fetchall()

    fetch = sync_session.execute(select_stmt, param)
    row = fetch.one()
    assert row == (new.name, new.float, new.decimal, new.datetime, new.boolean)


def test_fetchone(sync_cursor, model, records):
    select_stmt = sa.select(model).where(model.id == sa.bindparam("id"))
    sync_cursor.execute(select_stmt, {"id": records[0].id})
    row = sync_cursor.fetchone()
    assert row is not None
    assert isinstance(row, tuple)
    assert row == (
        records[0].id,
        records[0].name,
        records[0].float,
        records[0].decimal,
        records[0].datetime,
        records[0].boolean,
    )


def test_fetchmany(sync_cursor, model, records):
    size = (len(records) // 2) or 1
    select_stmt = sa.select(model).limit(sa.bindparam("size"))
    sync_cursor.execute(select_stmt, {"size": len(records)})
    rows = sync_cursor.fetchmany(size)
    assert len(rows) == size
    assert all(isinstance(x, tuple) for x in rows)


def test_fetchall(sync_cursor, model, records):
    size = len(records)
    select_stmt = sa.select(model).limit(sa.bindparam("size"))
    sync_cursor.execute(select_stmt, {"size": size})
    rows = sync_cursor.fetchall()
    assert len(rows) == size
    assert all(isinstance(x, tuple) for x in rows)


def test_executemany(sync_cursor, model, records):
    insert_stmt = sa.insert(model).values(
        name=sa.bindparam("name"),
        float=sa.bindparam("float"),
        decimal=sa.bindparam("decimal"),
        datetime=sa.bindparam("datetime"),
        boolean=sa.bindparam("boolean"),
    )
    values = [
        {
            "name": x.name,
            "float": x.float,
            "decimal": x.decimal,
            "datetime": x.datetime,
            "boolean": x.boolean,
        }
        for x in records
    ]

    sync_cursor.executemany(insert_stmt, values)

    # TODO: assert


async def test_rollback_async(async_session, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc),
        boolean=True,
    )
    async_session.add(new)
    await async_session.flush()
    await async_session.refresh(new)
    new_id = int(new.id)
    await async_session.rollback()
    maybe = await async_session.get(model, new_id)
    assert maybe is None


async def test_no_commit_async(async_engine, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc),
        boolean=True,
    )
    async with AsyncSession(async_engine) as session:
        session.add(new)
        await session.flush()
        await session.refresh(new)
        new_id = int(new.id)

    async with AsyncSession(async_engine) as session:
        maybe = await session.get(model, new_id)
        assert maybe is None


async def test_commit_async(async_engine, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc),
        boolean=True,
    )
    async with AsyncSession(async_engine) as session:
        session.add(new)
        await session.flush()
        await session.commit()
        new_id = int(new.id)

    async with AsyncSession(async_engine) as session:
        maybe = await session.get(model, new_id)
        assert maybe is not None

    assert new.id == maybe.id
    assert new.name == maybe.name
    assert new.float == maybe.float
    assert new.decimal == maybe.decimal
    assert new.datetime == maybe.datetime
    assert new.boolean is maybe.boolean


async def test_autocommit_async(async_raw_connection, async_session, model):
    new = model(
        name="test",
        float=1.0,
        decimal=Decimal("1.1"),
        datetime=datetime_class.now(timezone.utc).replace(tzinfo=None),
        boolean=True,
    )

    param = {
        "name": new.name,
        "float": new.float,
        "decimal": new.decimal,
        "datetime": new.datetime,
        "boolean": new.boolean,
    }
    insert_stmt = sa.insert(model).values(
        name=sa.bindparam("name"),
        float=sa.bindparam("float"),
        decimal=sa.bindparam("decimal"),
        datetime=sa.bindparam("datetime"),
        boolean=sa.bindparam("boolean"),
    )
    select_stmt = (
        sa.select(model)
        .with_only_columns(
            model.name, model.float, model.decimal, model.datetime, model.boolean
        )
        .where(
            model.name == sa.bindparam("name", type_=sa.String()),
            model.float == sa.bindparam("float", type_=sa.Float()),
            model.decimal == sa.bindparam("decimal", type_=sa.Numeric()),
            model.datetime
            == sa.bindparam("datetime", type_=sa.DateTime(timezone=False)),
            model.boolean == sa.bindparam("boolean", type_=sa.Boolean()),
        )
        .order_by(model.datetime.desc())
    )

    async_raw_connection.autocommit = True
    assert async_raw_connection.autocommit is True
    async with async_raw_connection.cursor() as cursor:
        await cursor.execute(insert_stmt, param)
        await cursor.fetchall()

    fetch = await async_session.execute(select_stmt, param)
    row = fetch.one()
    assert row == (new.name, new.float, new.decimal, new.datetime, new.boolean)


async def test_fetchone_async(async_cursor, model, records):
    select_stmt = sa.select(model).where(model.id == sa.bindparam("id"))
    await async_cursor.execute(select_stmt, {"id": records[0].id})
    row = await async_cursor.fetchone()
    assert row is not None
    assert isinstance(row, tuple)
    assert row == (
        records[0].id,
        records[0].name,
        records[0].float,
        records[0].decimal,
        records[0].datetime,
        records[0].boolean,
    )


async def test_fetchmany_async(async_cursor, model, records):
    size = (len(records) // 2) or 1
    select_stmt = sa.select(model).limit(sa.bindparam("size"))
    await async_cursor.execute(select_stmt, {"size": len(records)})
    rows = await async_cursor.fetchmany(size)
    assert len(rows) == size
    assert all(isinstance(x, tuple) for x in rows)


async def test_fetchall_async(async_cursor, model, records):
    size = len(records)
    select_stmt = sa.select(model).limit(sa.bindparam("size"))
    await async_cursor.execute(select_stmt, {"size": size})
    rows = await async_cursor.fetchall()
    assert len(rows) == size
    assert all(isinstance(x, tuple) for x in rows)


async def test_executemany_async(async_cursor, model, records):
    insert_stmt = sa.insert(model).values(
        name=sa.bindparam("name"),
        float=sa.bindparam("float"),
        decimal=sa.bindparam("decimal"),
        datetime=sa.bindparam("datetime"),
        boolean=sa.bindparam("boolean"),
    )
    values = [
        {
            "name": x.name,
            "float": x.float,
            "decimal": x.decimal,
            "datetime": x.datetime,
            "boolean": x.boolean,
        }
        for x in records
    ]

    await async_cursor.executemany(insert_stmt, values)

    # TODO: assert
