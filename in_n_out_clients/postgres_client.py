import datetime
import logging
from functools import partial
from typing import List

import pandas as pd
import sqlalchemy as db
from pandas.api.types import is_datetime64tz_dtype
from sqlalchemy import BOOLEAN, FLOAT, INTEGER, TIMESTAMP, VARCHAR

from in_n_out_clients.in_n_out_types import ConflictResolutionStrategy

# -- default values for use in upsert query when partial data provided: see
# `insert_with_conflict_resolution`
DEFAULT_VALUES_FOR_SQLALCHEMY_TYPES = {
    VARCHAR: "string",
    INTEGER: -1,
    FLOAT: -1.1,
    BOOLEAN: False,
}

logger = logging.getLogger(__file__)


class OnDataConflictFail(Exception):
    """Raised when there are conflicts in the data if on_data_conflict is
    ConflictResolutionStrategy.FAIL."""

    pass


class PostgresClient:
    """Client for interfacing with postgres databases.

    :param username: database username
    :param password: database password
    :param host: database host
    :param port: database port
    :param database_name: database name
    """

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
            self.engine = self.initialise_client()
        except db.exc.OperationalError as operational_error:
            raise ConnectionError(
                "Could not connect to postgres client. "
                f"Reason: {operational_error}"
            ) from operational_error

    def initialise_client(self):
        self.engine = db.create_engine(self.db_uri)
        return self.engine

    def query(self, query: str) -> pd.DataFrame:
        """Run a query against the databae.

        :param query: query to run
        :returns: dataframe of the query result (tested only for delect queries)
        """
        with self.engine.connect() as con:
            query_result = con.execute(db.text(query))
        data = query_result.fetchall()
        columns = list(query_result.keys())

        # TODO pandas hotfix: unable to understand date as a timevalue
        dtype_mapping = None
        for record in data:
            indices_of_date_items = [
                i
                for i, item in enumerate(record)
                if isinstance(item, datetime.date)
            ]
            dtype_mapping = {
                columns[i]: "datetime64[ns]" for i in indices_of_date_items
            }
            break

        df = pd.DataFrame.from_records(data, columns=columns)

        if dtype_mapping:
            df = df.astype(dtype_mapping)
        return df

    def _write(
        self,
        table_name: str,
        data: pd.DataFrame,
        on_data_conflict: str = "append",
        on_asset_conflict: str = "append",
        dataset_name: str | None = None,
        data_conflict_properties: List[str] | None = None,
    ):
        """Internal function that is used by `InNOutClient` as a universal
        write entry.

        :param table_name: name of the table to write to
        :param data: dataframe to write
        :param on_data_conflict: how to behave if some of the rows to
                write already exist, defaults to "append"
        :param on_asset_conflict: how to behave if the table already exists,
                defaults to "append"
        :param dataset_name: name of the dataset (postgres schema) that
                table belongs to, defaults to None
        :param data_conflict_properties: rows to check for conflicts.
                Note: these must match existing constraints on the
                table, defaults to None
        """
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
                self.engine,
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

        return {"status_code": 200, "msg": "successfully wrote data"}


def _generate_default_cols_when_partial_data(
    conn, table_name: str, columns_in_data: List[str]
):
    """Internal function to add default values to non-nullable columns without
    defaults from the target table in case where write data has partial
    columns.

    :param conn: SQLAlchemy connection
    :param table_name: name of the target table
    :param columns_in_data: these are the columns present in the `write`
        data, the function will ignore these columns
    :return: a list containing the metadata (to be directly used by
        `sqlalchemy.Column`) of all columns from the target table that
        by definition have no defaults and are not nullable with data-
        type inferred defaults (excludes columns present in data)
    """
    from sqlalchemy import Column, inspect

    all_columns = inspect(conn).get_columns(table_name)
    non_nullable_cols_with_no_default = []
    for column_metadata in all_columns:
        column_name = column_metadata["name"]
        is_nullable = column_metadata["nullable"]
        has_default = column_metadata["default"]
        if column_name in columns_in_data:
            continue
        if not is_nullable and has_default is None:
            logger.debug(
                (
                    f"Detected column `{column_name}` that is not present in "
                    "data, is not nullable and has no default value... adding "
                    "default value for purpose of upsert"
                )
            )
            column_type = column_metadata.pop("type")
            _type_class = column_type.__class__
            default_value = DEFAULT_VALUES_FOR_SQLALCHEMY_TYPES.get(
                _type_class
            )
            if default_value is None:
                logger.warning(
                    (
                        f"Column `{column_name}` has type {column_type} which "
                        "has no default-value inference support for the "
                        "purposes of upsert. Aborting partial column upsert..."
                    )
                )
                non_nullable_cols_with_no_default = []
                break
            column_metadata["default"] = default_value
            column_metadata["type_"] = column_type

            sqlalchemy_column = Column(**column_metadata)
            non_nullable_cols_with_no_default.append(sqlalchemy_column)
    return non_nullable_cols_with_no_default


def insert_with_conflict_resolution(
    table, conn, keys, data_iter, on_data_conflict, data_conflict_properties
):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row, strict=True)) for row in data_iter]

    sqlalchemy_table = table.table
    table_name = sqlalchemy_table.name

    insert_statement = insert(sqlalchemy_table).values(data)

    # -- find columns that need to be added to insert query in case of
    # partial data
    non_nullable_cols_with_no_default = (
        _generate_default_cols_when_partial_data(
            conn=conn, table_name=table_name, columns_in_data=keys
        )
    )
    for sqlalchemy_column in non_nullable_cols_with_no_default:
        sqlalchemy_table.append_column(sqlalchemy_column)

    match on_data_conflict:
        case ConflictResolutionStrategy.REPLACE:
            set_query = {
                c.key: c
                for c in insert_statement.excluded
                if c.key not in data_conflict_properties
            }

            for col in non_nullable_cols_with_no_default:
                set_query[col.key] = col

            stmt = insert_statement.on_conflict_do_update(
                index_elements=data_conflict_properties,
                set_=set_query,
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
            # TODO maybe can do a on_conflict_do_update query instead,
            # then return the
            # excluded values? E..g because at the moment the
            # returned data is a misnomer!
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

    import pandas as pd

    df = pd.DataFrame(
        {
            "currency": ["EUR", "GBP", "AED", "EUR", "AED"],
            "date": [
                "2025-01-01",
                "2024-01-01",
                "2026-01-01",
                "2017-01-01",
                "2012-01-01",
            ],
            "value_in_pounds": [-1.5, 0.9, 1, 20, -10],
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

    client.query("select * from currency_history").info()
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
