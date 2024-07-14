# jdbc-wrapper

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## ~~how to install~~
> not yet published
```shell
$ pip install jdbc-wrapper
# or
$ pip install "jdbc-wrapper @ git+https://github.com/phi-friday/jdbc-wrapper.git"
```

## TODO
* [ ] tests
* [ ] sqlalchemy dialect
* [ ] use `sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection`

## how to use
```bash
❯ wget https://github.com/xerial/sqlite-jdbc/releases/download/3.46.0.0/sqlite-jdbc-3.46.0.0.jar -O sqlite.jar
sqlite.jar                100%[==================================>]  12.98M  18.6MB/s    in 0.7s    
2024-07-13 19:45:32 (18.6 MB/s) - ‘sqlite.jar’ saved [13615436/13615436]

❯ wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.13/slf4j-api-2.0.13.jar -O slf4j.jar
slf4j.jar                 100%[==================================>]  67.00K   388KB/s    in 0.2s    
2024-07-13 19:45:35 (388 KB/s) - ‘slf4j.jar’ saved [68605/68605]

❯ rye run python ./example.py
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
[('StudentID', 'INTEGER', 2147483647, 2147483647, 0, 0, 1),
 ('Name', 'TEXT', 2147483647, 2147483647, 0, 0, 0),
 ('Major', 'TEXT', 2147483647, 2147483647, 0, 0, 1),
 ('Year', 'INTEGER', 2147483647, 2147483647, 0, 0, 1),
 ('GPA', 'REAL', 2147483647, 2147483647, 0, 0, 1)]
[(1, 'Alice', 'CS', 3, 3.5)]
[('date', 'TEXT', 2147483647, 2147483647, 0, 0, 1)]
[('2024-07-13 10:46:38',)]
```

### `example.py`
```python
from __future__ import annotations

from pprint import pprint

from jdbc_wrapper import connect


def main() -> None:
    with connect(
        "jdbc:sqlite::memory:",
        driver="org.sqlite.JDBC",
        modules=["sqlite.jar", "slf4j.jar"],
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
                INSERT INTO Students (Name, Major, Year, GPA)
                VALUES ('Alice', 'CS', 3, 3.5)
                """
            )

            cursor.execute("SELECT * FROM Students")
            keys1 = cursor.description
            rows1 = cursor.fetchall()

            cursor.execute("SELECT datetime('now') AS date;")
            keys2 = cursor.description
            rows2 = cursor.fetchall()

    pprint(keys1)
    pprint(rows1)
    pprint(keys2)
    pprint(rows2)


if __name__ == "__main__":
    main()

# output:
# SLF4J(W): No SLF4J providers were found.
# SLF4J(W): Defaulting to no-operation (NOP) logger implementation
# SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
# [('StudentID', 'INTEGER', 2147483647, 2147483647, 0, 0, 1),
#  ('Name', 'TEXT', 2147483647, 2147483647, 0, 0, 0),
#  ('Major', 'TEXT', 2147483647, 2147483647, 0, 0, 1),
#  ('Year', 'INTEGER', 2147483647, 2147483647, 0, 0, 1),
#  ('GPA', 'REAL', 2147483647, 2147483647, 0, 0, 1)]
# [(1, 'Alice', 'CS', 3, 3.5)]
# [('date', 'TEXT', 2147483647, 2147483647, 0, 0, 1)]
# [('2024-07-13 10:52:28',)]
```

### `sqlalchemy_example.py`
```python
from __future__ import annotations

from pprint import pprint

import sqlalchemy as sa
from sqlalchemy.orm import Session

import jdbc_wrapper  # noqa: F401

url = sa.make_url(
    "sqlite+jdbc_wrapper:///database.db?"
    "jdbc_driver=org.sqlite.JDBC&jdbc_modules=sqlite.jar&jdbc_modules=slf4j.jar"
)
engine = sa.create_engine(url)


def main() -> None:
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
    main()
# output:
# SLF4J(W): No SLF4J providers were found.
# SLF4J(W): Defaulting to no-operation (NOP) logger implementation
# SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
# RMKeyView(['StudentID', 'Name', 'Major', 'Year', 'GPA'])
# [(1, 'Alice', 'CS', 3, 3.5)]
# RMKeyView(['date'])
# [('2024-07-14 12:53:21',)]
```

## License

MIT, see [LICENSE](https://github.com/phi-friday/jdbc_wrapper/blob/main/LICENSE).
