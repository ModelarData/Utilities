import common

from pyarrow import flight
from pyarrow._flight import FlightClient


# Helper functions.
def initialize_database(flight_client: FlightClient, tables: list[str]) -> list[str]:
    result = common.do_action(flight_client, "InitializeDatabase", ",".join(tables))[0]
    decoded_result = result.body.to_pybytes().decode("utf-8")

    return decoded_result.split(";")


def execute_query(flight_client: FlightClient, query: str) -> None:
    # Retrieve the flight info that describes how to execute the query.
    query_descriptor = flight.FlightDescriptor.for_command(query)
    flight_info = flight_client.get_flight_info(query_descriptor)

    # Use the flight endpoint in the returned flight info to execute the query.
    endpoint = flight_info.endpoints[0]
    cloud_node_url = endpoint.locations[0]

    cloud_client = flight.FlightClient(cloud_node_url)
    common.do_get(cloud_client, endpoint.ticket)


if __name__ == "__main__":
    manager_client = flight.FlightClient("grpc://127.0.0.1:9998")
    common.create_test_tables(manager_client)

    execute_query(manager_client, "SELECT * FROM test_model_table_1 LIMIT 5")
