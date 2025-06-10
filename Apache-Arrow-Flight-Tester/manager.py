from typing import Literal

from pyarrow import flight
from pyarrow._flight import Result

from common import ModelarDBFlightClient
from protobuf import protocol_pb2
from server import ModelarDBServerFlightClient


class ModelarDBManagerFlightClient(ModelarDBFlightClient):
    """Functionality for interacting with a ModelarDB manager using Apache Arrow Flight."""

    def initialize_database(self, existing_tables: list[str]) -> protocol_pb2.TableMetadata:
        """
        Retrieve the table metadata required to initialize the database with the tables that are not included in the
        given list of tables. Throws an error if a table in the given list does not exist in the database.
        """
        database_metadata = protocol_pb2.DatabaseMetadata()
        database_metadata.table_names.extend(existing_tables)

        response = self.do_action("InitializeDatabase", database_metadata.SerializeToString())

        table_metadata = protocol_pb2.TableMetadata()
        table_metadata.ParseFromString(response[0].body.to_pybytes())

        return table_metadata

    def register_node(self, node_url: str, node_mode: Literal["cloud", "edge"]) -> protocol_pb2.ManagerMetadata:
        """Register a node with the given URL and mode in the manager."""
        node_metadata = protocol_pb2.NodeMetadata()
        node_metadata.url = node_url

        if node_mode == "cloud":
            node_metadata.server_mode = protocol_pb2.NodeMetadata.ServerMode.CLOUD
        else:
            node_metadata.server_mode = protocol_pb2.NodeMetadata.ServerMode.EDGE

        response = self.do_action("RegisterNode", node_metadata.SerializeToString())

        manager_metadata = protocol_pb2.ManagerMetadata()
        manager_metadata.ParseFromString(response[0].body.to_pybytes())

        return manager_metadata

    def remove_node(self, node_url: str) -> list[Result]:
        """Remove the node with the given URL from the manager."""
        node_metadata = protocol_pb2.NodeMetadata()
        node_metadata.url = node_url

        return self.do_action("RemoveNode", node_metadata.SerializeToString())

    def query(self, query: str) -> None:
        """
        Retrieve a cloud node that can execute the given query and execute the query on the node. It is assumed that
        at least one cloud node is already registered with the manager.
        """
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
    print(f"Node type: {manager_client.node_type()}\n")

    manager_client.create_test_tables()

    print(manager_client.initialize_database(["test_table_1"]))

    print(manager_client.register_node("grpc://127.0.0.1:9999", "edge"))
    print(manager_client.remove_node("grpc://127.0.0.1:9999"))

    manager_client.query("SELECT * FROM test_time_series_table_1 LIMIT 5")

    manager_client.clean_up_tables([], "drop")
