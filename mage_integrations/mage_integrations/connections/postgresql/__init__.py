from psycopg2 import connect
from psycopg2.extras import register_uuid

from mage_integrations.connections.sql.base import Connection


class PostgreSQL(Connection):
    def __init__(
        self,
        database: str,
        host: str,
        password: str,
        username: str,
        port: int = None,
        connection_factory=None,
    ):
        super().__init__()
        self.database = database
        self.host = host
        self.password = password
        self.port = port or 5432
        self.username = username
        self.connection_factory = connection_factory

    def build_connection(self):
        connect_kwargs = dict(
            dbname=self.database,
            host=self.host,
            password=self.password,
            port=self.port,
            user=self.username,
        )
        if self.connection_factory is not None:
            connect_kwargs['connection_factory'] = self.connection_factory
        connection = connect(**connect_kwargs)
        # uuid[] arrived as the array's literal text, as '{1111...,NULL}', since psycopg2
        # reads it only with a registered type; uuid values become text as before.
        register_uuid(conn_or_curs=connection)
        return connection
