from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from pprint import pprint

import sqlalchemy as sa
from sqlalchemy.orm import Session

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


def main(jdbc_connection_string: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        loader = PostgresqlLoader(base_dir=temp_dir)
        modules = loader.load_latest()

        url = create_url(jdbc_connection_string, loader.default_driver, *modules)
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
