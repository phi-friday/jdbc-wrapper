from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from pprint import pprint
from urllib.request import urlretrieve

import sqlalchemy as sa
from sqlalchemy.orm import Session

import jdbc_wrapper  # noqa: F401 # pyright: ignore[reportUnusedImport]


def create_url(jdbc_connection_string: str, *modules: Path) -> sa.engine.url.URL:
    url = sa.make_url("postgresql+jdbc_wrapper:///?jdbc_driver=org.postgresql.Driver")
    query = dict(url.query)
    query["jdbc_dsn"] = jdbc_connection_string
    query["jdbc_modules"] = tuple(str(module) for module in modules)
    return url.set(query=query)


def main(jdbc_connection_string: str, postgresql_jar_url: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        postgresql_jar = Path(temp_dir) / "postgresql.jar"
        urlretrieve(postgresql_jar_url, postgresql_jar)

        url = create_url(jdbc_connection_string, postgresql_jar)
        engine = sa.create_engine(url, poolclass=sa.NullPool)
        with Session(engine) as session:
            session.execute(
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
            session.execute(
                sa.text("""
                insert into "Students" ("Name", "Major", "Year", "GPA")
                values ('Alice', 'CS', 3, 3.5)
                """)
            )

            fetch = session.execute(sa.text('select * from "Students"'))
            keys1 = fetch.keys()
            rows1 = fetch.fetchall()

            fetch = session.execute(sa.text("select CURRENT_TIMESTAMP as date;"))
            keys2 = fetch.keys()
            rows2 = fetch.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)

    engine.dispose()


if __name__ == "__main__":
    main(*sys.argv[1:])
