from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import filelock
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine as sa_create_async_engine
from sqlalchemy.orm import Session

import jdbc_wrapper
from jdbc_wrapper._loader import find_loader
from jdbc_wrapper._loader import load as load_jdbc_modules

# isort: off
import anyio  # noqa: F401 # pyright: ignore[reportUnusedImport]
# isort: on

test_dir = Path(__file__).parent  # root/tests
cache_dir = test_dir.parent / ".cache"  # root/.cache


def _create_temp_dir() -> Path:
    temp_path = cache_dir / uuid.uuid4().hex  # root/.cache/uuid
    temp_path.mkdir(parents=True, exist_ok=True)
    return temp_path


def _clear_temp_dir(
    _temp_dir: Path, worker_id: str, *files: Path
) -> Generator[Path, None, None]:
    if worker_id == "master":
        try:
            yield _temp_dir
        finally:
            shutil.rmtree(_temp_dir)
        return

    self_dir = _temp_dir / "self"
    remain_dir = _temp_dir / "remain"
    self_dir.mkdir(exist_ok=True)
    remain_dir.mkdir(exist_ok=True)

    self_id = "".join(re.findall(r"\d+", worker_id))
    self_lock = self_dir / self_id
    remain_lock = remain_dir / self_id
    self_lock.touch(exist_ok=False)
    remain_lock.touch(exist_ok=False)
    maxima = int(os.environ["PYTEST_XDIST_WORKER_COUNT"])
    try:
        yield _temp_dir
    finally:
        clear_lock = _temp_dir / "clear.lock"
        with filelock.FileLock(clear_lock):
            workers = list(self_dir.glob("*"))
            remains = list(remain_dir.glob("*"))
            flag = (
                len(remains) == maxima
                and len(workers) == 1
                and workers[0].name == self_id
            )
            self_lock.unlink(missing_ok=False)

        if flag:
            shutil.rmtree(_temp_dir)
            for file in files:
                file.unlink(missing_ok=False)


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
def create_temp_dir(worker_id: str) -> Generator[Path, None, None]:
    if worker_id == "master":
        # only run once
        temp_dir = _create_temp_dir()
        yield from _clear_temp_dir(temp_dir, worker_id)
        return

    temp_dir_lock = cache_dir / "temp_dir.lock"
    temp_file = cache_dir / "temp_file"
    with filelock.FileLock(temp_dir_lock):
        if temp_file.is_file():
            with temp_file.open("r") as f:
                temp_dir = Path(f.read())
        else:
            temp_dir = _create_temp_dir()
            with temp_file.open("w+") as f:
                f.write(str(temp_dir))

    yield from _clear_temp_dir(temp_dir, worker_id, temp_file, temp_dir_lock)


@pytest.fixture(scope="session")
def temp_dir(create_temp_dir: Path) -> Path:
    return create_temp_dir


@pytest.fixture(scope="session")
def url_without_query(temp_dir: Path) -> sa.engine.url.URL:
    backend = os.getenv("DATABASE_BACKEND", "")
    username = os.getenv("DATABASE_USERNAME", "")
    password = os.getenv("DATABASE_PASSWORD", "")
    host = os.getenv("DATABASE_HOST", "")
    port_str = os.getenv("DATABASE_PORT", "")
    database = os.getenv("DATABASE_NAME", "")

    if not backend:
        # setup default
        backend = "sqlite"
        database_path = temp_dir / "test.db"
        database = str(database_path)

    port = int(port_str) if port_str else None

    return sa.engine.url.URL.create(
        drivername=f"{backend}+jdbc_wrapper",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )


@pytest.fixture(scope="session")
def url(
    url_without_query: sa.engine.url.URL, temp_dir: Path, worker_id: str
) -> sa.engine.url.URL:
    backend = url_without_query.get_backend_name().lower()
    module_dir = temp_dir / "jdbc_modules"
    module_dir.mkdir(exist_ok=True)
    if worker_id == "master":
        driver, modules = load_jdbc_modules(backend, module_dir)
        return url_without_query.set(
            query={"jdbc_modules": tuple(map(str, modules)), "jdbc_driver": driver}
        )

    module_lock = temp_dir / "jdbc_modules.lock"
    module_file = temp_dir / "jdbc_module_file"
    with filelock.FileLock(module_lock):
        if module_file.is_file():
            loader = find_loader(backend)
            with module_file.open("r") as f:
                modules = json.load(f)
            return url_without_query.set(
                query={"jdbc_modules": modules, "jdbc_driver": loader.default_driver}
            )
        driver, modules = load_jdbc_modules(backend, module_dir)
        modules = list(map(str, modules))
        with module_file.open("w+") as f:
            json.dump(modules, f)
        return url_without_query.set(
            query={"jdbc_modules": modules, "jdbc_driver": driver}
        )


@pytest.fixture(scope="session")
def create_sync_engine(
    url: sa.engine.url.URL,
) -> Generator[sa.engine.Engine, None, None]:
    engine = sa.create_engine(url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def sync_engine(create_sync_engine: sa.engine.Engine) -> sa.engine.Engine:
    return create_sync_engine


@pytest.fixture(scope="session")
async def create_async_engine(
    url: sa.engine.url.URL,
) -> AsyncGenerator[AsyncEngine, None]:
    engine = sa_create_async_engine(url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def async_engine(create_async_engine: AsyncEngine) -> AsyncEngine:
    return create_async_engine


@pytest.fixture()
def create_sync_raw_connection(
    sync_engine: sa.engine.Engine,
) -> Generator[jdbc_wrapper.Connection, None, None]:
    proxy = sync_engine.raw_connection()
    connection = proxy.driver_connection
    assert isinstance(connection, jdbc_wrapper.Connection)
    try:
        with connection:
            yield connection
    finally:
        proxy.close()


@pytest.fixture()
def sync_raw_connection(
    create_sync_raw_connection: jdbc_wrapper.Connection,
) -> jdbc_wrapper.Connection:
    return create_sync_raw_connection


@pytest.fixture()
async def create_async_raw_connection(
    async_engine: AsyncEngine,
) -> AsyncGenerator[jdbc_wrapper.AsyncConnection, None]:
    proxy = await async_engine.raw_connection()
    sa_connection = proxy.driver_connection
    assert isinstance(sa_connection, sa.AdaptedConnection)
    connection = sa_connection.driver_connection
    assert isinstance(connection, jdbc_wrapper.AsyncConnection)
    try:
        async with connection:
            yield connection
    finally:
        await connection.close()
        proxy.close()


@pytest.fixture()
async def async_raw_connection(
    create_async_raw_connection: jdbc_wrapper.AsyncConnection,
) -> jdbc_wrapper.AsyncConnection:
    return create_async_raw_connection


@pytest.fixture()
def create_sync_connection(
    sync_engine: sa.engine.Engine,
) -> Generator[sa.Connection, None, None]:
    with sync_engine.connect() as conn:
        yield conn


@pytest.fixture()
def sync_connection(create_sync_connection: sa.Connection) -> sa.Connection:
    return create_sync_connection


@pytest.fixture()
async def create_async_connection(
    async_engine: AsyncEngine,
) -> AsyncGenerator[AsyncConnection, None]:
    async with async_engine.connect() as conn:
        yield conn
        await conn.close()


@pytest.fixture()
def async_connection(create_async_connection: AsyncConnection) -> AsyncConnection:
    return create_async_connection


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
