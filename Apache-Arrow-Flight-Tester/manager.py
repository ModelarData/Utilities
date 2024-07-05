import common

from typing import Literal

from pyarrow import flight
from pyarrow._flight import FlightClient, Result


# Helper functions.
def initialize_database(flight_client: FlightClient, tables: list[str]) -> list[str]:
    result = common.do_action(flight_client, "InitializeDatabase", ",".join(tables))[0]
    decoded_result = result.body.to_pybytes().decode("utf-8")

    return decoded_result.split(";")


def register_node(flight_client: FlightClient, node_url: str, node_mode: Literal["cloud", "edge"]) -> list[Result]:
    encoded_node_url = common.encode_argument(node_url)
    encoded_node_mode = common.encode_argument(node_mode)

    action_body = encoded_node_url + encoded_node_mode
    action = flight.Action("RegisterNode", action_body)
    response = flight_client.do_action(action)

    return list(response)[0].body.to_pybytes()


def remove_node(flight_client: FlightClient, node_url: str) -> list[Result]:
    encoded_node_url = common.encode_argument(node_url)

    action = flight.Action("RemoveNode", encoded_node_url)
    response = flight_client.do_action(action)

    return list(response)


def execute_query(flight_client: FlightClient, query: str) -> None:
    print("Retrieving cloud node that can execute the query...")
    query_descriptor = flight.FlightDescriptor.for_command(query)
    flight_info = flight_client.get_flight_info(query_descriptor)

    endpoint = flight_info.endpoints[0]
    cloud_node_url = endpoint.locations[0]

    print(f"Executing query on {cloud_node_url}...")
    cloud_client = flight.FlightClient(cloud_node_url)
    common.do_get(cloud_client, endpoint.ticket)


if __name__ == "__main__":
    manager_client = flight.FlightClient("grpc://127.0.0.1:9998")
    common.create_test_tables(manager_client)

    execute_query(manager_client, "SELECT * FROM test_model_table_1 LIMIT 5")
