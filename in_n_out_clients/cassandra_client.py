# import uuid

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.cqltypes import SimpleDateType, UUIDType
from cassandra.query import dict_factory

DTYPE_MAP = {SimpleDateType: "datetime64[ns]", UUIDType: "str"}


class CassandraClient:
    def __init__(self, host: str):
        self.host = host

        self.cluster = self.initialise_client()  # TODO add error catching

    def initialise_client(self):
        self.cluster = Cluster([self.host])
        return self.cluster

    def query(self, query: str, dataset_name: str | None) -> pd.DataFrame:
        with self.cluster.connect() as conn:
            conn.row_factory = dict_factory
            if dataset_name is not None:
                conn.set_keyspace(dataset_name)
            rows = conn.execute(query)
            columns = rows.column_names
            column_types = rows.column_types
            df = pd.DataFrame(rows).astype(str)
            dtype_mapping = {}
            for column, column_type in zip(columns, column_types, strict=True):
                dtype = DTYPE_MAP.get(column_type)
                if dtype is not None:
                    dtype_mapping[column] = dtype

            if dtype_mapping:
                df = df.astype(dtype_mapping)


if __name__ == "__main__":
    client = CassandraClient("127.0.0.1")
    client.query("select * from person", "default")
    '''client.query(
        f"""
        insert into person
        (id, first_name, last_name, birthday)
        values ({uuid.uuid4()}, 'yousef', 'nami', '1999-07-22')""",
        "default",
    )'''
