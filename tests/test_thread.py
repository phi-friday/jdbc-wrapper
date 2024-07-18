# pyright: reportMissingParameterType=false
# pyright: reportUnknownParameterType=false
from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, wait

import pytest
import sqlalchemy as sa

_CONCURRENCY = 10


def test_thread_safe_connection(sync_raw_connection):
    def select_one(value: int):
        with sync_raw_connection.cursor() as cursor:
            cursor.execute("SELECT ?", [value])
            return cursor.fetchone()[0]

    with ThreadPoolExecutor(_CONCURRENCY) as pool:
        futures = [pool.submit(select_one, x) for x in range(_CONCURRENCY)]
        futures = wait(futures)
        result = {x.result() for x in futures.done}

    assert result == set(range(_CONCURRENCY))


def test_thread_concurrency(sync_engine, url):
    if url.get_backend_name().lower() != "postgresql":
        pytest.skip("This test is only for PostgreSQL")

    def do_sleep():
        with sync_engine.connect() as conn:
            fetch = conn.execute(sa.text("SELECT pg_sleep(1)"))
            fetch.fetchall()

    start = time.perf_counter()
    with ThreadPoolExecutor(_CONCURRENCY) as pool:
        futures = [pool.submit(do_sleep) for _ in range(_CONCURRENCY)]
        futures = wait(futures)
    end = time.perf_counter()
    diff = end - start

    assert diff < 2  # noqa: PLR2004


@pytest.mark.anyio()
async def test_thread_safe_connection_async(async_raw_connection):
    async def _select_one(value: int):
        async with async_raw_connection.cursor() as cursor:
            await cursor.execute("SELECT ?", [value])
            result = await cursor.fetchone()
            return result[0]

    def select_one(value: int):
        return asyncio.run(_select_one(value))

    with ThreadPoolExecutor(_CONCURRENCY) as pool:
        futures = [pool.submit(select_one, x) for x in range(_CONCURRENCY)]
        futures = wait(futures)
        result = {x.result() for x in futures.done}

    assert result == set(range(_CONCURRENCY))


@pytest.mark.anyio()
async def test_thread_concurrency_async(async_engine, url):
    if url.get_backend_name().lower() != "postgresql":
        pytest.skip("This test is only for PostgreSQL")

    async def _do_sleep():
        async with async_engine.connect() as conn:
            await conn.execute(sa.text("SELECT pg_sleep(1)"))

    def do_sleep():
        return asyncio.run(_do_sleep())

    start = time.perf_counter()
    with ThreadPoolExecutor(_CONCURRENCY) as pool:
        futures = [pool.submit(do_sleep) for _ in range(_CONCURRENCY)]
        futures = wait(futures)
    end = time.perf_counter()
    diff = end - start

    assert diff < 2  # noqa: PLR2004
