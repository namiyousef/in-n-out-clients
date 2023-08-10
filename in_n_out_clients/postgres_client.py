import pandas as pd
import sqlalchemy as db


class PostgresClient:
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        database_name: str,
        conflict_resolution_strategy: str = "append",
        dataset_name: str | None = None,
    ):
        self.db_user = username
        self.db_password = password
        self.db_host = host
        self.db_port = port
        self.db_name = database_name
        self.db_uri = (
            f"postgresql+psycopg2://{self.db_user}"
            f":{self.db_password}@{self.db_host}"
            f":{self.db_port}/{self.db_name}"
        )

    def initialise_client(self):
        self.engine = db.create_engine(self.db_uri)
        self.con = self.engine.connect()

    def query(self, query):
        query_result = self.con.execute(query)
        data = query_result.fetchall()
        columns = query_result.keys()
        df = pd.DataFrame(data, columns=columns)
        return df