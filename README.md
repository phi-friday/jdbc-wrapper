# pyjdbc2

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## how to install
```shell
$ pip install pyjdbc2
```

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

from pyjdbc2 import connect


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

## License

MIT, see [LICENSE](https://github.com/phi-friday/pyjdbc2/blob/main/LICENSE).
