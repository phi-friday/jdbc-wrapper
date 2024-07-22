# jdbc-wrapper

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## warning
This is a library I created for my own practice.
If you want to find a proper library, 
- try `psycopg` (aka `psycopg3`) for `postgresql`, 
- `pyodbc` and `aioodbc` for `mssql`,
- `pysqlite` and `aiosqlite` for `sqlite`.

## how to install
```shell
$ pip install jdbc-wrapper
# or
$ pip install "jdbc-wrapper[asyncio]"
# or
$ pip install "jdbc-wrapper[sqlalchemy]"
```

## how to use
see [examples](https://github.com/phi-friday/jdbc-wrapper/tree/main/examples)

## TODO
* [ ] more tests
> 1. support mssql
> 2. postgresql thread test in github action(why..?)
* [ ] support mssql(macos m1 docker issue...)
* [ ] more sqlalchemy dialect
* [ ] datetime, date, time type issue

## License

MIT, see [LICENSE](https://github.com/phi-friday/jdbc_wrapper/blob/main/LICENSE).
