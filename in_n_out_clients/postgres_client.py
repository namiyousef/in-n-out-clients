import logging
from typing import List

import pandas as pd
import sqlalchemy as db
from pandas.api.types import is_datetime64tz_dtype
from sqlalchemy import BOOLEAN, FLOAT, INTEGER, TIMESTAMP, VARCHAR

from in_n_out_clients.in_n_out_types import ConflictResolutionStrategy

logger = logging.getLogger(__file__)


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
        data_conflict_properties: List[str] | None = None
        # data_conflict_properties,
    ):
        resp = self.write(
            df=data,
            table_name=table_name,
            dataset_name=dataset_name,
            on_asset_conflict=on_asset_conflict,
            on_data_conflict=on_data_conflict,
            data_conflict_properties=data_conflict_properties,
        )

        return resp

    # conflict res should be a function of writing, not initialisation!
    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_name: str,
        on_asset_conflict: str,
        on_data_conflict: str,
        data_conflict_properties: List[str] | None = None,
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

        if on_data_conflict != ConflictResolutionStrategy.APPEND:
            if on_data_conflict == ConflictResolutionStrategy.REPLACE:
                raise NotImplementedError(
                    "There is currently no support for replace strategy"
                )
            if data_conflict_properties is None:
                data_conflict_properties = df.columns.tolist()

            select_columns = ",".join(data_conflict_properties)
            df_from_db = self.query(
                f"SELECT DISTINCT {select_columns} FROM {table_name}"
            )

            df = df.merge(df_from_db, how="left", indicator=True)

            df_conflicting_rows = df[df["_merge"] == "both"]
            df = df[df["_merge"] != "both"].drop("_merge", axis=1)

            if not df_conflicting_rows.empty:
                num_conflicting_rows = df_conflicting_rows.shape[0]
                logger.info(f"Found {num_conflicting_rows}...")
                match on_data_conflict:
                    case ConflictResolutionStrategy.FAIL:
                        logger.error(
                            "Exiting process since on_data_conflict=fail"
                        )
                        return {
                            "status_code": 409,
                            "msg": f"Found {num_conflicting_rows} that conflict",
                            "data": [
                                {
                                    "data_conflict_properties": (
                                        data_conflict_properties
                                    ),
                                    "first_5_conflicting_rows": (
                                        df_conflicting_rows.head()
                                        .astype(str)
                                        .to_dict(orient="records")
                                    ),
                                }
                            ],
                        }
                    case ConflictResolutionStrategy.IGNORE:
                        logger.info("Ignoring conflicting rows...")
            else:
                logger.info(
                    "No conflicts found... proceeding with normal write process"
                )

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
