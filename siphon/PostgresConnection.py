from sqlalchemy import create_engine
import os


class PostgresConnection(object):
    """
    Postgres database context manager
    """

    def __init__(self, database_var="LINKEDIN_DATABASE_URL", database_url=None):
        if database_url:
            self.database_url = database_url
        elif database_var:
            self.database_url = os.environ[database_var]
        else:
            raise Exception(
                "Please provide a database url or environmental database variable"
            )
        self.engine = None
        self.connection = None

    def __enter__(self):
        self.engine = create_engine(self.database_url)
        self.connection = self.engine.connect()
        self.connection.execute("set time zone 'UTC'")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()
        self.engine.dispose()
