from __future__ import annotations

import json
import os
import tempfile
import uuid
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any

import filelock
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine as sa_create_async_engine
from sqlalchemy.orm import Session

# isort: off
import anyio  # noqa: F401 # pyright: ignore[reportUnusedImport]
import jdbc_wrapper  # noqa: F401 # pyright: ignore[reportUnusedImport]
# isort: on

test_dir = Path(__file__).parent


def _create_temp_dir() -> Path:
    temp_dir = tempfile.gettempdir()
    temp_path = Path(temp_dir) / uuid.uuid4().hex
    temp_path.mkdir(parents=True, exist_ok=True)
    return temp_path


@pytest.fixture(scope="session", autouse=True)
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session", autouse=True)
def create_temp_dir(worker_id: str) -> Generator[Path, None, None]:
    if worker_id == "master":
        # only run once
        yield _create_temp_dir()
        return

    temp_dir_lock = test_dir / "temp_dir.lock"
    with filelock.FileLock(temp_dir_lock):
        if temp_dir_lock.is_file():
            with temp_dir_lock.open("r") as f:
                temp_dir = Path(f.read())
        else:
            temp_dir = _create_temp_dir()
            with temp_dir_lock.open("w+") as f:
                f.write(str(temp_dir))
        yield temp_dir


@pytest.fixture(scope="session", autouse=True)
def temp_dir(create_temp_dir: Path) -> Path:
    return create_temp_dir


@pytest.fixture(scope="session", autouse=True)
def url(temp_dir: Path) -> sa.engine.url.URL:
    backend = os.getenv("DATABASE_BACKEND", "")
    driver = os.getenv("DATABASE_DRIVER", "")
    driver_modules = os.getenv("DATABASE_DRIVER_MODULES", "")
    username = os.getenv("DATABASE_USERNAME", "")
    password = os.getenv("DATABASE_PASSWORD", "")
    host = os.getenv("DATABASE_HOST", "")
    port_str = os.getenv("DATABASE_PORT", "")
    query_str = os.getenv("DATABASE_QUERY", "{}")
    database = os.getenv("DATABASE_NAME", "")

    if not backend:
        # setup default
        backend = "sqlite"
        driver = "org.sqlite.JDBC"
        database_path = temp_dir / "test.db"
        database_path.touch(exist_ok=True)
        database = str(database_path)

    port = int(port_str) if port_str else None
    query: dict[str, Any] = json.loads(query_str)
    query["jdbc_driver"] = driver
    query["jdbc_modules"] = tuple(driver_modules.split(","))

    return sa.engine.url.URL.create(
        drivername=f"{backend}+jdbc_wrapper",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        query=query,
    )


@pytest.fixture(scope="session", autouse=True)
def create_sync_engine(
    url: sa.engine.url.URL,
) -> Generator[sa.engine.Engine, None, None]:
    engine = sa.create_engine(url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def sync_engine(create_sync_engine: sa.engine.Engine) -> sa.engine.Engine:
    return create_sync_engine


@pytest.fixture(scope="session", autouse=True)
async def create_async_engine(
    url: sa.engine.url.URL,
) -> AsyncGenerator[AsyncEngine, None]:
    engine = sa_create_async_engine(url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def async_engine(create_async_engine: AsyncEngine) -> AsyncEngine:
    return create_async_engine


@pytest.fixture()
def enter_sync_session(sync_engine: sa.engine.Engine) -> Generator[Session, None, None]:
    with Session(sync_engine) as session:
        yield session


@pytest.fixture()
def sync_session(enter_sync_session: Session) -> Session:
    return enter_sync_session


@pytest.fixture()
async def enter_async_session(
    async_engine: AsyncEngine,
) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_engine) as session:
        yield session


@pytest.fixture()
def async_session(enter_async_session: AsyncSession) -> AsyncSession:
    return enter_async_session
