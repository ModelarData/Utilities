import pprint

import pyarrow
from pyarrow import flight, Schema
from pyarrow._flight import FlightInfo, FlightClient, ActionType, Result, Ticket


# Apache Arrow Flight functions.
def list_flights(flight_client: FlightClient) -> list[FlightInfo]:
    response = flight_client.list_flights()

    return list(response)


def get_schema(flight_client: FlightClient, table_name: str) -> Schema:
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    response = flight_client.get_schema(upload_descriptor)

    return response.schema


def do_get(flight_client: FlightClient, ticket: Ticket) -> None:
    response = flight_client.do_get(ticket)

    for batch in response:
        pprint.pprint(batch.data.to_pydict())


def do_put(flight_client: FlightClient, table_name: str, record_batch: pyarrow.RecordBatch) -> None:
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, record_batch.schema)

    writer.write(record_batch)
    writer.close()


def do_action(flight_client: FlightClient, action_type: str, action_body: bytes) -> list[Result]:
    action = flight.Action(action_type, action_body)
    response = flight_client.do_action(action)

    return list(response)


def list_actions(flight_client: FlightClient) -> list[ActionType]:
    response = flight_client.list_actions()

    return list(response)


# Helper functions.
def list_table_names(flight_client: FlightClient) -> list[str]:
    flights = list_flights(flight_client)

    return [table_name.decode("utf-8") for table_name in flights[0].descriptor.path]


class ObjectStoreArguments:
    def __init__(self, **kwargs):
        self.arguments = kwargs

    def argument_values(self) -> list:
        return list(self.arguments.values())


class S3Arguments(ObjectStoreArguments):
    def __init__(self, endpoint: str, bucket_name: str, access_key_id: str, secret_access_key: str):
        super().__init__(endpoint=endpoint, bucket_name=bucket_name, access_key_id=access_key_id,
                         secret_access_key=secret_access_key)


class AzureBlobStorageArguments(ObjectStoreArguments):
    def __init__(self, account: str, access_key: str, container_name: str):
        super().__init__(account=account, access_key=access_key, container_name=container_name)


def update_object_store(flight_client: flight.FlightClient, object_store_arguments: ObjectStoreArguments) -> list[
    Result]:
    action_body = bytes()
    for argument in object_store_arguments.argument_values():
        action_body += encode_argument(argument)

    return do_action(flight_client, "UpdateRemoteObjectStore", action_body)


def encode_argument(argument: str) -> bytes:
    argument_bytes = str.encode(argument)
    argument_size = len(argument_bytes).to_bytes(2, byteorder="big")

    return argument_size + argument_bytes


def create_test_tables(flight_client: FlightClient) -> None:
    """
    Create a table and a model table using the flight client, print the current tables to ensure the created tables are
    included, and print the schema for the created table and model table to ensure the tables are created correctly.
    """
    print("Creating test tables...")
    print(do_action(
        flight_client,
        "CommandStatementUpdate",
        str.encode("CREATE TABLE test_table_1(timestamp TIMESTAMP, values REAL, metadata REAL)"),
    ))
    print(do_action(
        flight_client,
        "CommandStatementUpdate",
        str.encode("CREATE MODEL TABLE test_model_table_1(location TAG, install_year TAG, model "
                   "TAG, timestamp TIMESTAMP, power_output FIELD, wind_speed FIELD, temperature "
                   "FIELD(5%))"),
    ))

    print("\nCurrent tables:")
    table_names = list_table_names(flight_client)
    print(table_names)

    print(f"\nSchema for {table_names[0]}:")
    print(get_schema(flight_client, table_names[0]))

    print(f"\nSchema for {table_names[1]}:")
    print(get_schema(flight_client, table_names[1]))
