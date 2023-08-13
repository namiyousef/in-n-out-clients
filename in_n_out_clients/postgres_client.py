import pandas as pd
import sqlalchemy as db
from pandas.api.types import is_datetime64tz_dtype
from sqlalchemy import BOOLEAN, FLOAT, INTEGER, TIMESTAMP, VARCHAR


class PostgresClient:
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        database_name: str,
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
        try:
            self.engine, self.con = self.initialise_client()
        except db.exc.OperationalError as operational_error:
            raise ConnectionError(
                "Could not connect to postgres client. "
                f"Reason: {operational_error}"
            ) from operational_error

    def initialise_client(self):
        self.engine = db.create_engine(self.db_uri)
        self.con = self.engine.connect()
        return self.engine, self.con

    def query(self, query):
        query_result = self.con.execute(query)
        data = query_result.fetchall()
        columns = query_result.keys()
        df = pd.DataFrame(data, columns=columns)
        return df

    def _write(
        self,
        table_name: str,
        data,
        on_data_conflict: str = "append",
        on_asset_conflict: str = "append",
        dataset_name: str | None = None,
        # data_conflict_properties,
    ):
        resp = self.write(
            df=data,
            table_name=table_name,
            dataset_name=dataset_name,
            on_asset_conflict=on_asset_conflict,
        )

        return resp

    # conflict res should be a function of writing, not initialisation!
    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_name: str,
        on_asset_conflict: str,
    ):
        DTYPE_MAP = {
            "int64": INTEGER,
            "float64": FLOAT,
            "datetime64[ns]": TIMESTAMP,
            "datetime64[ns, UTC]": TIMESTAMP(timezone=True),
            "bool": BOOLEAN,
            "object": VARCHAR,
        }

        def _get_pg_datatypes(df):
            dtypes = {}
            for col, dtype in df.dtypes.items():
                if is_datetime64tz_dtype(dtype):
                    dtypes[col] = DTYPE_MAP["datetime64[ns, UTC]"]
                else:
                    dtypes[col] = DTYPE_MAP[str(dtype)]
            return dtypes

        dtypes = _get_pg_datatypes(df)

        df.to_sql(
            table_name,
            self.con,
            schema=dataset_name,
            if_exists=on_asset_conflict,
            index=False,
            method="multi",
            dtype=dtypes,
        )

        return {"status_code": 200, "msg": "successfully wrote data"}
