from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path
from pprint import pprint
from urllib.request import urlretrieve

from jdbc_wrapper import connect


async def main(jdbc_connection_string: str, postgresql_jar_url: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        postgresql_jar = Path(temp_dir) / "postgresql.jar"
        urlretrieve(postgresql_jar_url, postgresql_jar)

        async with connect(
            jdbc_connection_string,
            driver="org.postgresql.Driver",
            modules=[postgresql_jar],
            is_async=True,
        ) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE "Students" (
                        "StudentID" SERIAL PRIMARY KEY,
                        "Name" TEXT NOT NULL,
                        "Major" TEXT,
                        "Year" INTEGER,
                        "GPA" REAL
                    );
                """)
                await cursor.execute(
                    """
                    insert into "Students" ("Name", "Major", "Year", "GPA")
                    values ('Alice', 'CS', 3, 3.5)
                    """
                )

                await cursor.execute('select * from "Students"')
                keys1 = cursor.description
                rows1 = await cursor.fetchall()

                await cursor.execute("select CURRENT_TIMESTAMP as date;")
                keys2 = cursor.description
                rows2 = await cursor.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
