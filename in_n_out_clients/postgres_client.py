import logging
from typing import List

import pandas as pd
import sqlalchemy as db
from pandas.api.types import is_datetime64tz_dtype
from sqlalchemy import BOOLEAN, FLOAT, INTEGER, TIMESTAMP, VARCHAR

from in_n_out_clients.in_n_out_types import ConflictResolutionStrategy

logger = logging.getLogger(__file__)


class OnDataConflictFail(Exception):
    """Raised when there are conflicts in the data if on_data_conflict is
    ConflictResolutionStrategy.FAIL."""

    pass


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

        # -- define behavior:
        # if on_data_conflict !=APPEND, AND no conflict columns provided...!
        # Rejects this altogether. Do not want to allow this case
        # We would be making the behaviour ambiguous.
        # -- if we have provided conflict columns, then as per postgres
        if on_data_conflict != ConflictResolutionStrategy.APPEND:
            method = partial(
                insert_with_conflict_resolution,
                on_data_conflict=on_data_conflict,
                data_conflict_properties=data_conflict_properties,
            )
        else:
            method = "multi"

        try:
            df.to_sql(
                table_name,
                self.con,
                schema=dataset_name,
                if_exists=on_asset_conflict,
                index=False,
                method=method,
                dtype=dtypes,
            )
        except OnDataConflictFail as on_data_conflict_fail:
            logger.error("Exiting process since on_data_conflict=fail")
            return {"status_code": 409, **on_data_conflict_fail.args[0]}

            # -- below is the old method for conflict resolution!
            """if on_data_conflict == ConflictResolutionStrategy.REPLACE:
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
                )"""

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


def insert_with_conflict_resolution(
    table, conn, keys, data_iter, on_data_conflict, data_conflict_properties
):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row, strict=True)) for row in data_iter]

    insert_statement = insert(table.table).values(data)

    match on_data_conflict:
        case ConflictResolutionStrategy.REPLACE:
            # def _exclude_columns(column):
            #    pass

            stmt = insert_statement.on_conflict_do_update(
                index_elements=data_conflict_properties,
                set_={
                    c.key: c
                    for c in insert_statement.excluded
                    if c not in data_conflict_properties
                },
            )
        case _:
            stmt = insert_statement.on_conflict_do_nothing(
                index_elements=data_conflict_properties
            )

    result = conn.execute(stmt)
    num_results = result.rowcount

    if on_data_conflict == ConflictResolutionStrategy.FAIL:
        if num_results != len(data):
            conn.rollback()
            raise OnDataConflictFail(
                {
                    "msg": f"Found {len(data) - num_results} conflicting rows",
                    "data": [
                        {
                            "data_conflict_properties": (
                                data_conflict_properties
                            ),
                            "first_5_conflicting_rows": (str(data[:5])),
                        }
                    ],
                }
            )

    return num_results


def postgres_fail():
    pass


if __name__ == "__main__":
    client = PostgresClient(
        "postgres", "postgres", "localhost", 5432, "postgres"
    )

    from functools import partial

    import pandas as pd

    df = pd.DataFrame(
        {
            "currency": ["EUR", "GBP"],
            "date": ["2025-01-01", "2024-01-01"],
            "value_in_pounds": [-1, 0.9],
        }
    ).astype({"date": "datetime64[ns]"})
    print(
        client.write(
            df,
            "currency_history",
            "public",
            "append",
            "fail",
            ["currency", "date"],
        )
    )
    raise Exception()
    df.to_sql(
        "currency_history",
        client.con,
        schema="public",
        if_exists="append",
        index=False,
        method=partial(
            insert_with_conflict_resolution,
            data_conflict_properties=["currency", "date"],
            on_data_conflict="fail",
        ),
        chunksize=1,
    )
