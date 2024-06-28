import pprint
import time
from random import randrange
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


def do_put(flight_client: FlightClient, table_name: str, num_rows: int) -> None:
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, get_pyarrow_schema())

    record_batch = create_record_batch(num_rows)
    writer.write(record_batch)
    writer.close()


def do_action(flight_client: FlightClient, action_type: str, action_body: str) -> list[Result]:
    action_body_bytes = str.encode(action_body)
    action = flight.Action(action_type, action_body_bytes)
    response = flight_client.do_action(action)

    return list(response)


def list_actions(flight_client: FlightClient) -> list[ActionType]:
    response = flight_client.list_actions()

    return list(response)


# Helper functions.
def get_pyarrow_schema() -> pyarrow.Schema:
    return pyarrow.schema(
        [
            ("location", pyarrow.utf8()),
            ("install_year", pyarrow.utf8()),
            ("model", pyarrow.utf8()),
            ("timestamp", pyarrow.timestamp("ms")),
            ("power_output", pyarrow.float32()),
            ("wind_speed", pyarrow.float32()),
            ("temperature", pyarrow.float32()),
        ]
    )


def create_record_batch(num_rows: int) -> pyarrow.RecordBatch:
    location = ["aalborg" if i % 2 == 0 else "nibe" for i in range(num_rows)]
    install_year = ["2021" if i % 2 == 0 else "2022" for i in range(num_rows)]
    model = ["w72" if i % 2 == 0 else "w73" for i in range(num_rows)]

    timestamp = [round(time.time() * 1000) + (i * 1000) for i in range(num_rows)]
    power_output = [float(randrange(0, 30)) for _ in range(num_rows)]
    wind_speed = [float(randrange(50, 100)) for _ in range(num_rows)]
    temperature = [float(randrange(0, 40)) for _ in range(num_rows)]

    return pyarrow.RecordBatch.from_arrays(
        [
            location,
            install_year,
            model,
            timestamp,
            power_output,
            wind_speed,
            temperature,
        ],
        schema=get_pyarrow_schema(),
    )


def list_table_names(flight_client: FlightClient) -> list[str]:
    flights = list_flights(flight_client)

    return [table_name.decode("utf-8") for table_name in flights[0].descriptor.path]


def initialize_database(flight_client: FlightClient, tables: list[str]) -> list[str]:
    result = do_action(flight_client, "InitializeDatabase", ",".join(tables))[0]
    decoded_result = result.body.to_pybytes().decode("utf-8")

    return decoded_result.split(";")


def execute_query_with_manager(flight_client: FlightClient, query: str) -> None:
    # Retrieve the flight info that describes how to execute the query.
    query_descriptor = flight.FlightDescriptor.for_command(query)
    flight_info = flight_client.get_flight_info(query_descriptor)

    # Use the flight endpoint in the returned flight info to execute the query.
    endpoint = flight_info.endpoints[0]
    cloud_node_url = endpoint.locations[0]

    cloud_client = flight.FlightClient(cloud_node_url)
    do_get(cloud_client, endpoint.ticket)


def update_configuration(flight_client: flight.FlightClient, setting: str, setting_value: str) -> list[Result]:
    setting_bytes = str.encode(setting)
    setting_size = len(setting_bytes).to_bytes(2, byteorder="big")

    setting_value_bytes = str.encode(setting_value)
    setting_value_size = len(setting_value_bytes).to_bytes(2, byteorder="big")

    action_body = setting_size + setting_bytes + setting_value_size + setting_value_bytes
    action = flight.Action("UpdateConfiguration", action_body)
    response = flight_client.do_action(action)

    return list(response)


def update_object_store(flight_client: flight.FlightClient, object_store_type: Literal["s3", "azureblobstorage"],
                        arguments: list[str]) -> list[Result]:
    """
    Update the remote object store in the flight client to the given object store type with the given arguments.
    If `object_store_type` is `s3`, the arguments should be endpoint, bucket name, access key ID, and secret access
    key. If `object_store_type` is `azureblobstorage`, the arguments should be account, access key, and container name.
    """
    arguments.insert(0, object_store_type)
    action_body = create_update_object_store_action_body(arguments)

    result = flight_client.do_action(flight.Action("UpdateRemoteObjectStore", action_body))

    return list(result)


def create_update_object_store_action_body(arguments: list[str]) -> bytes:
    action_body = bytes()

    for argument in arguments:
        argument_bytes = str.encode(argument)
        argument_size = len(argument_bytes).to_bytes(2, byteorder="big")

        action_body += argument_size + argument_bytes

    return action_body


# Main Function.
if __name__ == "__main__":
    manager_client = flight.FlightClient("grpc://127.0.0.1:9998")

    print(list_actions(manager_client))

    print(do_action(
        manager_client,
        "CommandStatementUpdate",
        "CREATE TABLE test_table_1(timestamp TIMESTAMP, values REAL, metadata REAL)",
    ))
    print(do_action(
        manager_client,
        "CommandStatementUpdate",
        "CREATE MODEL TABLE test_model_table_1(location TAG, install_year TAG, model"
        " TAG, timestamp TIMESTAMP, power_output FIELD, wind_speed FIELD, temperature"
        " FIELD(5%))",
    ))

    print(list_table_names(manager_client))
    print(get_schema(manager_client, "test_table_1"))
    print(get_schema(manager_client, "test_model_table_1"))

    print(initialize_database(manager_client, ["test_table_1"]))
