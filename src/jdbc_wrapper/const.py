from __future__ import annotations

from jpype import dbapi2 as jpype_dbapi2

API_LEVEL = jpype_dbapi2.apilevel
THREAD_SAFETY = jpype_dbapi2.threadsafety
PARAM_STYLE = jpype_dbapi2.paramstyle

#

JDBC_QUERY_DSN = "jdbc_dsn"
JDBC_QUERY_DRIVER = "jdbc_driver"
JDBC_QUERY_MODULES = "jdbc_modules"
