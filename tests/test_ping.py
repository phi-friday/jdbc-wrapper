# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

import sqlalchemy as sa


def test_ping(sync_session):
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
