from __future__ import annotations

import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from pprint import pprint
from urllib.request import urlretrieve

from jdbc_wrapper import connect


def main(sqlite_jar_url: str, slf4j_jar_url: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        sqlite_jar = Path(temp_dir) / "sqlite.jar"
        slf4j_jar = sqlite_jar.with_name("slf4j.jar")

        with ThreadPoolExecutor(2) as pool:
            future0 = pool.submit(urlretrieve, sqlite_jar_url, sqlite_jar)
            future1 = pool.submit(urlretrieve, slf4j_jar_url, slf4j_jar)
            wait([future0, future1], return_when="ALL_COMPLETED")

        with connect(
            "jdbc:sqlite::memory:",
            driver="org.sqlite.JDBC",
            modules=[sqlite_jar, slf4j_jar],
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE Students (
                        StudentID INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL,
                        Major TEXT,
                        Year INTEGER,
                        GPA REAL
                    );
                """)
                cursor.execute(
                    """
                    insert into Students (Name, Major, Year, GPA)
                    values ('Alice', 'CS', 3, 3.5)
                    """
                )

                cursor.execute("select * from Students")
                keys1 = cursor.description
                rows1 = cursor.fetchall()

                cursor.execute("select datetime('now') as date;")
                keys2 = cursor.description
                rows2 = cursor.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)


if __name__ == "__main__":
    main(*sys.argv[1:])
