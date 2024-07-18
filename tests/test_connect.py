# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

import pytest
import sqlalchemy as sa


def test_ping1(sync_session):
    stmt = sa.text("select 1")
    fetch = sync_session.execute(stmt)
    value = fetch.scalar_one()
    assert isinstance(value, int)
    assert value == 1


def test_ping2(sync_session):
    stmt = sa.text("select 1")
    fetch = sync_session.execute(stmt)
    value = fetch.scalar_one()
    assert isinstance(value, int)
    assert value == 1


@pytest.mark.anyio()
async def test_ping3(async_session):
    stmt = sa.text("select 1")
    fetch = await async_session.execute(stmt)
    value = fetch.scalar_one()
    assert isinstance(value, int)
    assert value == 1


@pytest.mark.anyio()
async def test_ping4(async_session):
    stmt = sa.text("select 1")
    fetch = await async_session.execute(stmt)
    value = fetch.scalar_one()
    assert isinstance(value, int)
    assert value == 1
