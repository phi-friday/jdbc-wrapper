from __future__ import annotations

import asyncio
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from pprint import pprint
from urllib.request import urlretrieve

from jdbc_wrapper import connect


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

        async with connect(
            "jdbc:sqlite::memory:",
            driver="org.sqlite.JDBC",
            modules=[sqlite_jar, slf4j_jar],
            is_async=True,
        ) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE Students (
                        StudentID INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL,
                        Major TEXT,
                        Year INTEGER,
                        GPA REAL
                    );
                """)
                await cursor.execute(
                    """
                    insert into Students (Name, Major, Year, GPA)
                    values ('Alice', 'CS', 3, 3.5)
                    """
                )

                await cursor.execute("select * from Students")
                keys1 = cursor.description
                rows1 = await cursor.fetchall()

                await cursor.execute("select datetime('now') as date;")
                keys2 = cursor.description
                rows2 = await cursor.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
