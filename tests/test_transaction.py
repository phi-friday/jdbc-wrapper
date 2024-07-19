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


def test_autocommit(sync_raw_connection, sync_engine, sync_session, model):
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
    insert_stmt = str(
        sa.insert(model)
        .values(param)
        .compile(sync_engine, compile_kwargs={"literal_binds": True})
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
        cursor.execute(insert_stmt)
        cursor.fetchall()

    fetch = sync_session.execute(select_stmt, param)
    row = fetch.one()
    assert row == (new.name, new.float, new.decimal, new.datetime, new.boolean)


@pytest.mark.anyio()
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


@pytest.mark.anyio()
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


@pytest.mark.anyio()
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


@pytest.mark.anyio()
async def test_autocommit_async(
    async_raw_connection, async_engine, async_session, model
):
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
    insert_stmt = str(
        sa.insert(model)
        .values(param)
        .compile(async_engine, compile_kwargs={"literal_binds": True})
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
        await cursor.execute(insert_stmt)
        await cursor.fetchall()

    fetch = await async_session.execute(select_stmt, param)
    row = fetch.one()
    assert row == (new.name, new.float, new.decimal, new.datetime, new.boolean)
