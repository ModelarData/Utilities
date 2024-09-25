from typing import Literal

from pyarrow import flight
from pyarrow._flight import Result

from common import ModelarDBFlightClient, encode_argument
from server import ModelarDBServerFlightClient


class ModelarDBManagerFlightClient(ModelarDBFlightClient):
    """Functionality for interacting with a ModelarDB manager using Apache Arrow Flight."""

    def initialize_database(self, existing_tables: list[str]) -> list[str]:
        """
        Retrieve the SQL statements required to initialize the database with the tables that are not included in the
        given list of tables. Throws an error if a table in the given list does not exist in the database.
        """
        result = self.do_action("InitializeDatabase", str.encode(",".join(existing_tables)))[0]
        decoded_result = result.body.to_pybytes().decode("utf-8")

        return decoded_result.split(";")

    def register_node(self, node_url: str, node_mode: Literal["cloud", "edge"]) -> list[Result]:
        """Register a node with the given URL and mode in the manager."""
        encoded_node_url = encode_argument(node_url)
        encoded_node_mode = encode_argument(node_mode)

        action_body = encoded_node_url + encoded_node_mode
        response = self.do_action("RegisterNode", action_body)

        return response[0].body.to_pybytes()

    def remove_node(self, node_url: str) -> list[Result]:
        """Remove the node with the given URL from the manager"""
        encoded_node_url = encode_argument(node_url)

        return self.do_action("RemoveNode", encoded_node_url)

    def query(self, query: str) -> None:
        """Retrieve a cloud node that can execute the given query and execute the query on the node."""
        print("Retrieving cloud node that can execute the query...")
        query_descriptor = flight.FlightDescriptor.for_command(query)
        flight_info = self.flight_client.get_flight_info(query_descriptor)

        endpoint = flight_info.endpoints[0]
        cloud_node_url = endpoint.locations[0]

        print(f"Executing query on {cloud_node_url}...")
        cloud_client = ModelarDBServerFlightClient(cloud_node_url)
        cloud_client.do_get(endpoint.ticket)


if __name__ == "__main__":
    manager_client = ModelarDBManagerFlightClient("grpc://127.0.0.1:9998")
    manager_client.create_test_tables()

    print(manager_client.initialize_database(["test_table_1"]))

    print(manager_client.register_node("grpc://127.0.0.1:9999", "edge"))
    print(manager_client.remove_node("grpc://127.0.0.1:9999"))

    manager_client.query("SELECT * FROM test_model_table_1 LIMIT 5")
