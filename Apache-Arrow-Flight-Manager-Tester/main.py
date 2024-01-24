import pprint
from typing import Literal

from pyarrow import flight, Schema
from pyarrow._flight import FlightInfo, FlightClient, ActionType, FlightEndpoint


# PyArrow Functions.
def list_flights(flight_client: FlightClient) -> list[FlightInfo]:
    response = flight_client.list_flights()

    return list(response)


def get_schema(flight_client: FlightClient, table_name: str) -> Schema:
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    response = flight_client.get_schema(upload_descriptor)

    return response.schema


def do_action(flight_client: FlightClient, action_type: str, action_body: str) -> list[any]:
    action_body_bytes = str.encode(action_body)
    action = flight.Action(action_type, action_body_bytes)
    response = flight_client.do_action(action)

    return list(response)


def list_actions(flight_client: FlightClient) -> list[ActionType]:
    response = flight_client.list_actions()

    return list(response)


# Helper functions.
def list_table_names(flight_client: FlightClient) -> list[str]:
    flights = list_flights(flight_client)

    return [table_name.decode("utf-8") for table_name in flights[0].descriptor.path]


def initialize_database(flight_client: FlightClient, tables: list[str]) -> list[str]:
    result = do_action(flight_client, "InitializeDatabase", ",".join(tables))[0]
    decoded_result = result.body.to_pybytes().decode("utf-8")

    return decoded_result.split(";")


def execute_query(flight_client: FlightClient, query: str) -> None:
    # Retrieve the flight info that describes how to execute the query.
    query_descriptor = flight.FlightDescriptor.for_command(query)
    flight_info: FlightInfo = flight_client.get_flight_info(query_descriptor)

    # Use the flight endpoint in the returned flight info to execute the query.
    endpoint: FlightEndpoint = flight_info.endpoints[0]
    cloud_node_url = endpoint.locations[0]

    cloud_client = flight.FlightClient(cloud_node_url)
    response = cloud_client.do_get(endpoint.ticket)

    for batch in response:
        pprint.pprint(batch.data.to_pydict())


def update_object_store(flight_client: flight.FlightClient, object_store_type: Literal["s3", "azureblobstorage"],
                        arguments: list[str]) -> list[any]:
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
        " FIELD(5))",
    ))

    print(list_table_names(manager_client))
    print(get_schema(manager_client, "test_table_1"))
    print(get_schema(manager_client, "test_model_table_1"))

    print(initialize_database(manager_client, ["test_table_1"]))
