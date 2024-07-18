# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

from datetime import datetime as datetime_class
from datetime import timezone
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
)

metadata = sa.MetaData()


class Base(DeclarativeBase, MappedAsDataclass): ...


class Table(Base, kw_only=True):
    __tablename__ = "test_table"
    metadata = metadata

    id: Mapped[int] = mapped_column(init=False, primary_key=True, autoincrement=True)
    name: Mapped[str]
    float: Mapped[float]
    decimal: Mapped[Decimal] = mapped_column(sa.Numeric(precision=10, scale=2))
    datetime: Mapped[datetime_class] = mapped_column(sa.DateTime(timezone=True))
    boolean: Mapped[bool]


@pytest.fixture(scope="module", autouse=True)
def table(sync_engine) -> sa.Table:
    with sync_engine.begin() as conn:
        metadata.create_all(conn, checkfirst=True)
    return Table.__table__  # pyright: ignore[reportReturnType]


def test_rollback(sync_session):
    new = Table(
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
    maybe = sync_session.get(Table, new_id)
    assert maybe is None


def test_no_commit(sync_engine):
    new = Table(
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
        maybe = session.get(Table, new_id)
        assert maybe is None


def test_commit(sync_engine):
    new = Table(
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
        maybe = session.get(Table, new_id)
        assert maybe is not None

    assert new.id == maybe.id
    assert new.name == maybe.name
    assert new.float == maybe.float
    assert new.decimal == maybe.decimal
    assert new.datetime == maybe.datetime
    assert new.boolean is maybe.boolean


@pytest.mark.anyio()
async def test_rollback_async(async_session):
    new = Table(
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
    maybe = await async_session.get(Table, new_id)
    assert maybe is None


@pytest.mark.anyio()
async def test_no_commit_async(async_engine):
    new = Table(
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
        maybe = await session.get(Table, new_id)
        assert maybe is None


@pytest.mark.anyio()
async def test_commit_async(async_engine):
    new = Table(
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
        maybe = await session.get(Table, new_id)
        assert maybe is not None

    assert new.id == maybe.id
    assert new.name == maybe.name
    assert new.float == maybe.float
    assert new.decimal == maybe.decimal
    assert new.datetime == maybe.datetime
    assert new.boolean is maybe.boolean
