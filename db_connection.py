from sqlalchemy import create_engine, text


class DBConnection:
    def __init__(self, user, password, host, port, dbname):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.engine = None

    def create_database_if_not_exists(self):
        # Connect to the default 'postgres' database to check/create our target DB.
        default_conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/postgres"
        engine_default = create_engine(default_conn_str)
        with engine_default.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{self.dbname}'"))
            exists = result.scalar() is not None
            if not exists:
                conn.execute(text(f"CREATE DATABASE {self.dbname}"))
                print(f"[DBConnection] Database '{self.dbname}' created.")
            else:
                print(f"[DBConnection] Database '{self.dbname}' already exists.")

    def connect(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )
        print("[DBConnection] Engine created.")
        return self.engine
