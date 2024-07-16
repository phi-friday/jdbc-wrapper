from __future__ import annotations

from jpype import dbapi2 as jpype_dbapi2

API_LEVEL = jpype_dbapi2.apilevel
THREAD_SAFETY = jpype_dbapi2.threadsafety
PARAM_STYLE = jpype_dbapi2.paramstyle

#

JDBC_QUERY_DSN = "jdbc_dsn"
JDBC_QUERY_DRIVER = "jdbc_driver"
JDBC_QUERY_MODULES = "jdbc_modules"

#
# sqlite
DEFAULT_SQLITE_JDBC_DRIVER = "org.sqlite.JDBC"
DEFAULT_SQLITE_JDBC_OWNER = "xerial"
DEFAULT_SQLITE_JDBC_REPO = "sqlite-jdbc"
SLF4J_JDBC_OWNER = "qos-ch"
SLF4J_JDBC_REPO = "slf4j"
SLF4J_MAVEN_URL = "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/{version}/slf4j-api-{version}.jar"
# postgresql
DEFAULT_POSTGRESQL_JDBC_DRIVER = "org.postgresql.Driver"
DEFAULT_POSTGRESQL_JDBC_OWNER = "pgjdbc"
DEFAULT_POSTGRESQL_JDBC_REPO = "pgjdbc"
# mssql
DEFAULT_MSSQL_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
DEFAULT_MSSQL_JDBC_OWNER = "microsoft"
DEFAULT_MSSQL_JDBC_REPO = "mssql-jdbc"
