from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path
from pprint import pprint

import sqlalchemy as sa
from sqlalchemy.ext import asyncio as sa_asyncio

from jdbc_wrapper._loader import PostgresqlLoader


def create_url(
    jdbc_connection_string: str, driver: str, *modules: Path
) -> sa.engine.url.URL:
    url = sa.make_url("postgresql+jdbc_wrapper://")
    query = {
        "jdbc_dsn": jdbc_connection_string,
        "jdbc_driver": driver,
        "jdbc_modules": tuple(str(module) for module in modules),
    }
    return url.set(query=query)


async def main(jdbc_connection_string: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        loader = PostgresqlLoader(base_dir=temp_dir)
        modules = loader.load_latest()

        url = create_url(jdbc_connection_string, loader.default_driver, *modules)
        engine = sa_asyncio.create_async_engine(url)
        async with sa_asyncio.AsyncSession(engine) as session:
            await session.execute(
                sa.text("""
                CREATE TABLE "Students" (
                    "StudentID" SERIAL PRIMARY KEY,
                    "Name" TEXT NOT NULL,
                    "Major" TEXT,
                    "Year" INTEGER,
                    "GPA" REAL
                );
            """)
            )
            await session.execute(
                sa.text("""
                insert into "Students" ("Name", "Major", "Year", "GPA")
                values ('Alice', 'CS', 3, 3.5)
                """)
            )

            fetch = await session.execute(sa.text('select * from "Students"'))
            keys1 = fetch.keys()
            rows1 = fetch.fetchall()

            fetch = await session.execute(sa.text("select CURRENT_TIMESTAMP as date;"))
            keys2 = fetch.keys()
            rows2 = fetch.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
