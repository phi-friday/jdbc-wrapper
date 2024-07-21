# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

pytestmark = pytest.mark.anyio


def test_rollback(sync_session, model):
    new = model()
    sync_session.add(new)
    sync_session.flush()
    sync_session.refresh(new)
    new_id = int(new.id)
    sync_session.rollback()
    maybe = sync_session.get(model, new_id)
    assert maybe is None


def test_no_commit(sync_engine, model):
    new = model()
    with Session(sync_engine) as session:
        session.add(new)
        session.flush()
        session.refresh(new)
        new_id = int(new.id)

    with Session(sync_engine) as session:
        maybe = session.get(model, new_id)
        assert maybe is None


def test_commit(sync_engine, model):
    new = model()
    with Session(sync_engine) as session:
        session.add(new)
        session.flush()
        session.commit()
        new_id = int(new.id)

    with Session(sync_engine) as session:
        maybe = session.get(model, new_id)
        assert maybe is not None

    for key in model.__dataclass_fields__:
        assert getattr(new, key) == getattr(maybe, key)


def test_autocommit(sync_raw_connection, sync_session, model):
    new = model()
    insert_param = {
        "name": new.name,
        "float": new.float,
        "decimal": new.decimal,
        "datetime": new.datetime,
        "date": new.date,
        "time": new.time,
        "boolean": new.boolean,
        "unique": new.unique,
    }
    select_param = {"unique": new.unique}
    insert_stmt = sa.insert(model).values(
        name=sa.bindparam("name"),
        float=sa.bindparam("float"),
        decimal=sa.bindparam("decimal"),
        datetime=sa.bindparam("datetime"),
        date=sa.bindparam("date"),
        time=sa.bindparam("time"),
        boolean=sa.bindparam("boolean"),
        unique=sa.bindparam("unique"),
    )
    select_stmt = (
        sa.select(model)
        .with_only_columns(
            model.name,
            model.float,
            model.decimal,
            model.datetime,
            model.date,
            model.time,
            model.boolean,
            model.unique,
        )
        .where(model.unique == sa.bindparam("unique", type_=sa.String()))
        .order_by(model.datetime.desc())
    )

    sync_raw_connection.autocommit = True
    assert sync_raw_connection.autocommit is True
    with sync_raw_connection.cursor() as cursor:
        cursor.execute(insert_stmt, insert_param)
        cursor.fetchall()

    fetch = sync_session.execute(select_stmt, select_param)
    row = fetch.one()
    assert row == (
        new.name,
        new.float,
        new.decimal,
        new.datetime,
        new.date,
        new.time,
        new.boolean,
        new.unique,
    )


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
        records[0].date,
        records[0].time,
        records[0].boolean,
        records[0].unique,
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
        date=sa.bindparam("date"),
        time=sa.bindparam("time"),
        boolean=sa.bindparam("boolean"),
        unique=sa.bindparam("unique"),
    )
    values = [
        {
            "name": x.name,
            "float": x.float,
            "decimal": x.decimal,
            "datetime": x.datetime,
            "date": x.date,
            "time": x.time,
            "boolean": x.boolean,
            "unique": f"{x.unique}_{idx}",
        }
        for idx, x in enumerate(records)
    ]

    sync_cursor.executemany(insert_stmt, values)

    # TODO: assert


async def test_rollback_async(async_session, model):
    new = model()
    async_session.add(new)
    await async_session.flush()
    await async_session.refresh(new)
    new_id = int(new.id)
    await async_session.rollback()
    maybe = await async_session.get(model, new_id)
    assert maybe is None


async def test_no_commit_async(async_engine, model):
    new = model()
    async with AsyncSession(async_engine) as session:
        session.add(new)
        await session.flush()
        await session.refresh(new)
        new_id = int(new.id)

    async with AsyncSession(async_engine) as session:
        maybe = await session.get(model, new_id)
        assert maybe is None


async def test_commit_async(async_engine, model):
    new = model()
    async with AsyncSession(async_engine) as session:
        session.add(new)
        await session.flush()
        await session.commit()
        new_id = int(new.id)

    async with AsyncSession(async_engine) as session:
        maybe = await session.get(model, new_id)
        assert maybe is not None

    for key in model.__dataclass_fields__:
        assert getattr(new, key) == getattr(maybe, key)


async def test_autocommit_async(async_raw_connection, async_session, model):
    new = model()
    insert_param = {
        "name": new.name,
        "float": new.float,
        "decimal": new.decimal,
        "datetime": new.datetime,
        "date": new.date,
        "time": new.time,
        "boolean": new.boolean,
        "unique": new.unique,
    }
    select_param = {"unique": new.unique}
    insert_stmt = sa.insert(model).values(
        name=sa.bindparam("name"),
        float=sa.bindparam("float"),
        decimal=sa.bindparam("decimal"),
        datetime=sa.bindparam("datetime"),
        date=sa.bindparam("date"),
        time=sa.bindparam("time"),
        boolean=sa.bindparam("boolean"),
        unique=sa.bindparam("unique"),
    )
    select_stmt = (
        sa.select(model)
        .with_only_columns(
            model.name,
            model.float,
            model.decimal,
            model.datetime,
            model.date,
            model.time,
            model.boolean,
            model.unique,
        )
        .where(model.unique == sa.bindparam("unique", type_=sa.String()))
        .order_by(model.datetime.desc())
    )

    async_raw_connection.autocommit = True
    assert async_raw_connection.autocommit is True
    async with async_raw_connection.cursor() as cursor:
        await cursor.execute(insert_stmt, insert_param)
        await cursor.fetchall()

    fetch = await async_session.execute(select_stmt, select_param)
    row = fetch.one()
    assert row == (
        new.name,
        new.float,
        new.decimal,
        new.datetime,
        new.date,
        new.time,
        new.boolean,
        new.unique,
    )


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
        records[0].date,
        records[0].time,
        records[0].boolean,
        records[0].unique,
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
        date=sa.bindparam("date"),
        time=sa.bindparam("time"),
        boolean=sa.bindparam("boolean"),
        unique=sa.bindparam("unique"),
    )
    values = [
        {
            "name": x.name,
            "float": x.float,
            "decimal": x.decimal,
            "datetime": x.datetime,
            "date": x.date,
            "time": x.time,
            "boolean": x.boolean,
            "unique": f"{x.unique}_{idx}",
        }
        for idx, x in enumerate(records)
    ]

    await async_cursor.executemany(insert_stmt, values)

    # TODO: assert
