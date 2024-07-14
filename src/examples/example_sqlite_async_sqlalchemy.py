from __future__ import annotations

import asyncio
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from pprint import pprint
from urllib.request import urlretrieve

import sqlalchemy as sa
from sqlalchemy.ext import asyncio as sa_asyncio

import jdbc_wrapper  # noqa: F401 # pyright: ignore[reportUnusedImport]


def create_url(*modules: Path) -> sa.engine.url.URL:
    url = sa.make_url(
        "sqlite+jdbc_async_wrapper:///database.db?jdbc_driver=org.sqlite.JDBC"
    )
    query = dict(url.query)
    query["jdbc_modules"] = tuple(str(module) for module in modules)
    return url.set(query=query)


async def main(sqlite_jar_url: str, slf4j_jar_url: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        sqlite_jar = Path(temp_dir) / "sqlite.jar"
        slf4j_jar = sqlite_jar.with_name("slf4j.jar")

        with ThreadPoolExecutor(2) as pool:
            future0 = pool.submit(urlretrieve, sqlite_jar_url, sqlite_jar)
            future1 = pool.submit(urlretrieve, slf4j_jar_url, slf4j_jar)
            future0 = asyncio.wrap_future(future0)
            future1 = asyncio.wrap_future(future1)
            futures = asyncio.gather(future0, future1)
            await futures

        url = create_url(sqlite_jar, slf4j_jar)
        engine = sa_asyncio.create_async_engine(url)
        async with sa_asyncio.AsyncSession(engine) as session:
            await session.execute(
                sa.text("""
                CREATE TABLE Students (
                    StudentID INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    Major TEXT,
                    Year INTEGER,
                    GPA REAL
                );
            """)
            )
            await session.execute(
                sa.text("""
                insert into Students (Name, Major, Year, GPA)
                values ('Alice', 'CS', 3, 3.5)
                """)
            )

            fetch = await session.execute(sa.text("select * from Students"))
            keys1 = fetch.keys()
            rows1 = fetch.fetchall()

            fetch = await session.execute(sa.text("select datetime('now') as date;"))
            keys2 = fetch.keys()
            rows2 = fetch.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
