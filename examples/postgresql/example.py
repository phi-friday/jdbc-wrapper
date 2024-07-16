from __future__ import annotations

import sys
import tempfile
from pprint import pprint

from jdbc_wrapper import connect
from jdbc_wrapper._loader import PostgresqlLoader


def main(jdbc_connection_string: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        loader = PostgresqlLoader(base_dir=temp_dir)
        modules = loader.load_latest()

        with connect(
            jdbc_connection_string, driver=loader.default_driver, modules=modules
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE "Students" (
                        "StudentID" SERIAL PRIMARY KEY,
                        "Name" TEXT NOT NULL,
                        "Major" TEXT,
                        "Year" INTEGER,
                        "GPA" REAL
                    );
                """)
                cursor.execute(
                    """
                    insert into "Students" ("Name", "Major", "Year", "GPA")
                    values ('Alice', 'CS', 3, 3.5)
                    """
                )

                cursor.execute('select * from "Students"')
                keys1 = cursor.description
                rows1 = cursor.fetchall()

                cursor.execute("select CURRENT_TIMESTAMP as date;")
                keys2 = cursor.description
                rows2 = cursor.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)


if __name__ == "__main__":
    main(*sys.argv[1:])
