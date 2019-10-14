#!/usr/bin/env python3

import backoff
import _mssql
# import pymssql
import pyodbc

import singer
# import ssl

LOGGER = singer.get_logger()

DEFAULT_PORT = 1433
DEFAULT_TDS_VERSION = "7.3"
DEFAULT_CHARSET = "utf8"

@backoff.on_exception(backoff.expo,
                      (_mssql.MSSQLDatabaseException),
                      max_tries=5,
                      factor=2)
def connect_with_backoff(connection):
    server = connection.args["server"]
    database = connection.args['database']
    username = connection.args["user"]
    password = connection.args["password"]
    driver = '{ODBC Driver 17 for SQL Server}'
    conn = pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

    return conn

class MSSQLConnection:
    def __init__(self, config):
        self.args = {
            "server": config["server"],
            "user": config["user"],
            "password": config["password"],
            "database": config["database"],
            "port": config.get("port", DEFAULT_PORT),
            "tds_version": config.get("tds_version", DEFAULT_TDS_VERSION),
            "charset": config.get("charset", DEFAULT_CHARSET),
        }

    def connect(self):
        kwargs = self.args
        self.connection = _mssql.MSSQLConnection(**kwargs)
        return self.connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *exc_info):
        del exc_info
        self.connection.close()

def make_connection_wrapper(config):
    # pylint: disable=too-few-public-methods
    class ConnectionWrapper(MSSQLConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(config)
            connect_with_backoff(self)

    return ConnectionWrapper
