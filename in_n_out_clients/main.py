import inspect
import logging

from in_n_out_clients.google_calendar_client import GoogleCalendarClient
from in_n_out_clients.postgres_client import PostgresClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_TYPE_TO_CLIENT_MAPPING = {
    "pg": {
        "client_class": PostgresClient,
    },
    "google_calendar": {"client_class": GoogleCalendarClient},
    "bq": {"client_class": None},
}


def get_function_parameters(method, exclude_self=True):
    method_signature = inspect.signature(method)
    params = []
    for param_name, param_class in method_signature.parameters.items():
        annotation = param_class.annotation
        default = param_class.default
        param_metadata = {"param_name": param_name}

        if default is not inspect._empty:
            param_metadata.update(
                {"is_required": False, "default_value": default}
            )
        else:
            param_metadata["is_required"] = True

        if annotation is not inspect._empty:
            param_metadata["param_annotation"] = annotation

        params.append(param_metadata)

    if exclude_self:
        params = params[1:]

    return params


for database_type in DATABASE_TYPE_TO_CLIENT_MAPPING:
    client_class = DATABASE_TYPE_TO_CLIENT_MAPPING[database_type][
        "client_class"
    ]
    if client_class is not None:
        write_method = DATABASE_TYPE_TO_CLIENT_MAPPING[database_type][
            "client_class"
        ]._write
        write_method_params = get_function_parameters(write_method)
    DATABASE_TYPE_TO_CLIENT_MAPPING[database_type][
        "write_method_params"
    ] = write_method_params


class InNOutClient:
    """Universal CLient to connect to different services :param database_type:

    type of service to connect to
    :param database_name: name of the service to
    connect to, if applicable
    :param password: password of the service to
    connect to, if applicable
    :param username: username of the service to
    connect to, if applicable
    :param host: host of the service to connect to,
    if applicable
    :param port: port of the service to connect to, if
    applicable.
    """

    def __init__(
        self,
        database_type: str,
        database_name: str | None = None,
        password: str | None = None,
        username: str | None = None,
        host: str | None = None,
        port: int | None = None,
    ):
        connection_params = {}
        nullable_params = {
            "database_name": database_name,
            "password": password,
            "username": username,
            "host": host,
            "port": port,
        }

        nullable_params = {
            param_name: param_value
            for param_name, param_value in nullable_params.items()
            if param_value is not None
        }

        connection_params.update(nullable_params)

        logger.info(f"Connecting to `{database_type}` client...")
        self.client = self._connect_to_client(
            database_type=database_type, connection_params=connection_params
        )

        self.database_type = database_type
        self.database_name = database_name
        self.password = password
        self.username = username
        self.host = host
        self.port = port

    # should raise an error if fails
    def _connect_to_client(self, database_type: str, connection_params: dict):
        """Function to connect to a client.

        :param database_type: type of service to connect to
        :param connection_params: connection params to the service
        """
        client = DATABASE_TYPE_TO_CLIENT_MAPPING.get(database_type)
        if client is None:
            raise NotImplementedError(
                f"database_type={database_type} is not a valid client"
            )
        client_class = client["client_class"]
        client_instance = client_class(**connection_params)
        return client_instance

    def create_asset(self):
        pass

    # this function is to be used solely for writing data
    # TBH not sure if on_asset_conflict makes sense... is there a situation
    # where you would want to write to something... and then if it does exist
    # you raise a conflict??

    # like maybe maybe for a case of creating data from scratch only???

    def write(
        self,
        table_name: str,
        data,
        dataset_name: str | None = None,
        on_data_conflict: str = "append",
        on_asset_conflict: str = "append",
        data_conflict_properties: list | None = None,
    ):
        """Generic function to write data to any resource. Note that the
        purpose of this is solely to write data to an existing resource.

        :param table_name: name of the table to write data to
        :param data: the data to write, can be of any format
        :param dataset_name: name of the dataset to write to, if any,
            defaults to None
        :param on_data_conflict: how to behave if there is a conflict,
            defaults to "append"
        :param on_asset_conflict: how to behave if there is an asset conflict,
            defaults to "append"
        :param data_conflict_properties: what properties to check for conflicts

        Note: data can be of any type, not limited to dataframes. This is done
        to plan for the future when we add more clients!
        """
        logger.debug(
            (
                "Filtering inputs to `write` method to match those required "
                f"by `_write` method for `{self.database_type}` client..."
            )
        )
        input_arguments = {
            "table_name": table_name,
            "data": data,
            "dataset_name": dataset_name,
            "on_data_conflict": on_data_conflict,
            "data_conflict_properties": data_conflict_properties,
            "on_asset_conflict": on_asset_conflict,
        }

        write_method_params = DATABASE_TYPE_TO_CLIENT_MAPPING[
            self.database_type
        ]["write_method_params"]

        filtered_params = {}
        for param_metadata in write_method_params:
            is_required = param_metadata["is_required"]
            param_name = param_metadata["param_name"]
            param_value = input_arguments.get(param_name)
            if param_value is not None:
                filtered_params[param_name] = param_value
            elif is_required:
                raise Exception(
                    (
                        f"Client for `{self.database_type}` requires "
                        f"parameter `{param_name}` but this is missing "
                        "from InNOutClient.write inputs... "
                        "this is a bug and needs to be looked into."
                    )
                )

        logger.debug(
            f"Calling `_write` method of `{self.database_type}` client..."
        )

        resp = self.client._write(**filtered_params)

        return resp

    def read(
        self,
    ):
        pass


if __name__ == "__main__":
    client = InNOutClient("google_calendar")

    client.write(
        "yousefofthenamis@gmail.com",
        {},
        "testing",
    )
