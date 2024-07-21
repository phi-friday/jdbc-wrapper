from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from collections.abc import AsyncGenerator, Generator
from datetime import date as date_class
from datetime import datetime as datetime_class
from datetime import time as time_class
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import filelock
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine as sa_create_async_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
)

import jdbc_wrapper
from jdbc_wrapper import const as jdbc_wrapper_const
from jdbc_wrapper._loader import find_loader
from jdbc_wrapper._loader import load as load_jdbc_modules
from jdbc_wrapper.utils import url_to_dsn

test_dir = Path(__file__).parent  # root/tests
cache_dir = test_dir.parent / ".cache"  # root/.cache

metadata = sa.MetaData()


class Base(DeclarativeBase, MappedAsDataclass): ...


class Table(Base, kw_only=True):
    __tablename__ = "test_table"
    metadata = metadata

    id: Mapped[int] = mapped_column(init=False, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(default="test")
    float: Mapped[float] = mapped_column(default_factory=lambda: 1.1)
    decimal: Mapped[Decimal] = mapped_column(
        sa.Numeric(precision=10, scale=2), default_factory=lambda: Decimal("2.2")
    )
    datetime: Mapped[datetime_class] = mapped_column(
        sa.DateTime(timezone=False),
        default_factory=lambda: (x := datetime_class.now(timezone.utc)).replace(
            tzinfo=None, microsecond=x.microsecond // 1000 * 1000
        ),
    )
    date: Mapped[date_class] = mapped_column(
        sa.Date(), default_factory=lambda: datetime_class.now(timezone.utc).date()
    )
    time: Mapped[time_class] = mapped_column(
        sa.Time(timezone=False),
        default_factory=lambda: datetime_class.now(timezone.utc)
        .time()
        .replace(tzinfo=None, microsecond=0),
    )
    boolean: Mapped[bool] = mapped_column(default=True)
    unique: Mapped[str] = mapped_column(
        sa.String(255), unique=True, default_factory=lambda: uuid.uuid4().hex
    )


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
    url = os.getenv("DATABASE_URL", "")
    if url:
        return sa.make_url(url)

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
        modules = os.environ.get("DATABASE_JDBC_MODULES", "")
        driver = os.environ.get("DATABASE_JDBC_DRIVER", "")
        if modules and driver:
            return url_without_query.set(
                query={
                    "jdbc_modules": [
                        str(Path(x.strip()).resolve()) for x in modules.split(",")
                    ],
                    "jdbc_driver": driver.strip(),
                }
            )

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

        modules = os.environ.get("DATABASE_JDBC_MODULES", "")
        driver = os.environ.get("DATABASE_JDBC_DRIVER", "")
        if modules and driver:
            modules = [str(Path(x.strip()).resolve()) for x in modules.split(",")]
            driver = driver.strip()
        else:
            driver, modules = load_jdbc_modules(backend, module_dir)
        modules = list(map(str, modules))
        with module_file.open("w+") as f:
            json.dump(modules, f)
        return url_without_query.set(
            query={"jdbc_modules": modules, "jdbc_driver": driver}
        )


@pytest.fixture(scope="session")
def jdbc_driver(url: sa.engine.url.URL) -> str:
    driver = url.query["jdbc_driver"]
    assert isinstance(driver, str)
    return driver


@pytest.fixture(scope="session")
def jdbc_modules(url: sa.engine.url.URL) -> tuple[str, ...] | str:
    return url.query["jdbc_modules"]


@pytest.fixture(scope="session")
def jdbc_dsn_parts(url: sa.engine.url.URL) -> tuple[str, dict[str, Any]]:
    dsn, query = url_to_dsn(url)
    return dsn, dict(query)


@pytest.fixture(scope="session")
def jdbc_dsn(jdbc_dsn_parts: tuple[str, dict[str, Any]]) -> str:
    return jdbc_dsn_parts[0]


@pytest.fixture(scope="session")
def jdbc_driver_args(jdbc_dsn_parts: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    ignore = {
        jdbc_wrapper_const.JDBC_QUERY_DRIVER,
        jdbc_wrapper_const.JDBC_QUERY_MODULES,
        jdbc_wrapper_const.JDBC_QUERY_DRIVER,
    }
    return {key: value for key, value in jdbc_dsn_parts[1].items() if key not in ignore}


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
def table(sync_engine: sa.engine.Engine) -> sa.Table:
    with sync_engine.begin() as conn:
        metadata.create_all(conn, checkfirst=True)
    return Table.__table__  # pyright: ignore[reportReturnType]


@pytest.fixture(scope="session")
def model(table) -> type[Table]:  # type: ignore # noqa: ARG001
    return Table


@pytest.fixture(scope="session")
def records(sync_engine: sa.engine.Engine, model: type[Table]) -> list[Table]:
    records = [model(float=x, decimal=Decimal(f"{x}.{x}")) for x in range(10)]
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
    return list(records)


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
    jdbc_dsn: str,
    jdbc_driver: str,
    jdbc_modules: str | tuple[str, ...],
    jdbc_driver_args: dict[str, Any],
) -> Generator[jdbc_wrapper.Connection, None, None]:
    with jdbc_wrapper.connect(
        jdbc_dsn, jdbc_driver, jdbc_modules, jdbc_driver_args, is_async=False
    ) as connection:
        yield connection


@pytest.fixture()
def sync_raw_connection(
    create_sync_raw_connection: jdbc_wrapper.Connection,
) -> jdbc_wrapper.Connection:
    return create_sync_raw_connection


@pytest.fixture()
def create_sync_cursor(
    sync_raw_connection: jdbc_wrapper.Connection,
) -> Generator[jdbc_wrapper.Cursor, None, None]:
    with sync_raw_connection.cursor() as cursor:
        yield cursor


@pytest.fixture()
def sync_cursor(create_sync_cursor: jdbc_wrapper.Cursor) -> jdbc_wrapper.Cursor:
    return create_sync_cursor


@pytest.fixture()
async def create_async_raw_connection(
    jdbc_dsn: str,
    jdbc_driver: str,
    jdbc_modules: str | tuple[str, ...],
    jdbc_driver_args: dict[str, Any],
) -> AsyncGenerator[jdbc_wrapper.AsyncConnection, None]:
    async with jdbc_wrapper.connect(
        jdbc_dsn, jdbc_driver, jdbc_modules, jdbc_driver_args, is_async=True
    ) as connection:
        yield connection


@pytest.fixture()
async def async_raw_connection(
    create_async_raw_connection: jdbc_wrapper.AsyncConnection,
) -> jdbc_wrapper.AsyncConnection:
    return create_async_raw_connection


@pytest.fixture()
async def create_async_cursor(
    async_raw_connection: jdbc_wrapper.AsyncConnection,
) -> AsyncGenerator[jdbc_wrapper.AsyncCursor, None]:
    async with async_raw_connection.cursor() as cursor:
        yield cursor


@pytest.fixture()
async def async_cursor(
    create_async_cursor: jdbc_wrapper.AsyncCursor,
) -> jdbc_wrapper.AsyncCursor:
    return create_async_cursor


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
