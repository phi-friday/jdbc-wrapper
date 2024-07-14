from __future__ import annotations

import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from pprint import pprint
from urllib.request import urlretrieve

import sqlalchemy as sa
from sqlalchemy.orm import Session

import jdbc_wrapper  # noqa: F401 # pyright: ignore[reportUnusedImport]


def create_url(*modules: Path) -> sa.engine.url.URL:
    url = sa.make_url("sqlite+jdbc_wrapper:///?jdbc_driver=org.sqlite.JDBC")
    query = dict(url.query)
    query["jdbc_modules"] = tuple(str(module) for module in modules)
    return url.set(query=query)


def main(sqlite_jar_url: str, slf4j_jar_url: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        sqlite_jar = Path(temp_dir) / "sqlite.jar"
        slf4j_jar = sqlite_jar.with_name("slf4j.jar")

        with ThreadPoolExecutor(2) as pool:
            future0 = pool.submit(urlretrieve, sqlite_jar_url, sqlite_jar)
            future1 = pool.submit(urlretrieve, slf4j_jar_url, slf4j_jar)
            wait([future0, future1], return_when="ALL_COMPLETED")

        url = create_url(sqlite_jar, slf4j_jar)
        engine = sa.create_engine(url, poolclass=sa.NullPool)
        with Session(engine) as session:
            session.execute(
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
            session.execute(
                sa.text("""
                insert into Students (Name, Major, Year, GPA)
                values ('Alice', 'CS', 3, 3.5)
                """)
            )

            fetch = session.execute(sa.text("select * from Students"))
            keys1 = fetch.keys()
            rows1 = fetch.fetchall()

            fetch = session.execute(sa.text("select datetime('now') as date;"))
            keys2 = fetch.keys()
            rows2 = fetch.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)


if __name__ == "__main__":
    main(*sys.argv[1:])
