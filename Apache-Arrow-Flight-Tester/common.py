import pprint
from typing import Literal

import pyarrow
from pyarrow import flight, Schema
from pyarrow._flight import FlightInfo, FlightClient, ActionType, Result, Ticket


# PyArrow Functions.
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


def update_object_store(flight_client: flight.FlightClient, object_store_type: Literal["s3", "azureblobstorage"],
                        arguments: list[str]) -> list[Result]:
    """
    Update the remote object store in the flight client to the given object store type with the given arguments.
    If `object_store_type` is `s3`, the arguments should be endpoint, bucket name, access key ID, and secret access
    key. If `object_store_type` is `azureblobstorage`, the arguments should be account, access key, and container name.
    """
    arguments.insert(0, object_store_type)

    action_body = bytes()
    for argument in arguments:
        action_body += encode_argument(argument)

    return do_action(flight_client, "UpdateRemoteObjectStore", action_body)


def encode_argument(argument: str) -> bytes:
    argument_bytes = str.encode(argument)
    argument_size = len(argument_bytes).to_bytes(2, byteorder="big")

    return argument_size + argument_bytes


def create_test_tables(flight_client: FlightClient) -> None:
    """
    Create a table and a model table in the flight client, print the current tables to ensure the created tables are
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
        str.encode("CREATE MODEL TABLE test_model_table_1(location TAG, install_year TAG, model"
                   "TAG, timestamp TIMESTAMP, power_output FIELD, wind_speed FIELD, temperature"
                   "FIELD(5%))"),
    ))

    print("\nCurrent tables:")
    table_names = list_table_names(flight_client)
    print(table_names)

    print(f"\nSchema for {table_names[0]}:")
    print(get_schema(flight_client, table_names[0]))

    print(f"\nSchema for {table_names[1]}:")
    print(get_schema(flight_client, table_names[1]))
