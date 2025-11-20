import pyarrow
from pyarrow import flight
from pyarrow._flight import Result

from common import ModelarDBFlightClient
from protobuf import protocol_pb2
from util import ingest_into_server_and_query_table, create_test_tables, clean_up_tables


class ModelarDBServerFlightClient(ModelarDBFlightClient):
    """Functionality for interacting with a ModelarDB server using Apache Arrow Flight."""

    def do_put(self, table_name: str, record_batch: pyarrow.RecordBatch) -> None:
        """Insert the data in the given record batch into the table with the given table name."""
        upload_descriptor = flight.FlightDescriptor.for_path(table_name)
        writer, _ = self.flight_client.do_put(upload_descriptor, record_batch.schema)

        writer.write(record_batch)
        writer.close()

    def get_configuration(self) -> protocol_pb2.Configuration:
        """Get the current configuration of the server."""
        response = self.do_action("GetConfiguration", b"")

        configuration = protocol_pb2.Configuration()
        configuration.ParseFromString(response[0].body.to_pybytes())

        return configuration

    def update_configuration(self, setting: protocol_pb2.UpdateConfiguration.Setting,
                             new_value: int) -> list[Result]:
        """Update the given setting to the given new value in the server configuration."""
        update_configuration = protocol_pb2.UpdateConfiguration()
        update_configuration.setting = setting
        update_configuration.new_value = new_value

        return self.do_action("UpdateConfiguration", update_configuration.SerializeToString())

    def workload_balanced_query(self, query: str) -> None:
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
    server_client = ModelarDBServerFlightClient("grpc://127.0.0.1:9999")
    print(f"Node type: {server_client.node_type()}\n")

    create_test_tables(server_client)
    ingest_into_server_and_query_table(server_client, "test_time_series_table_1", 10000)

    print("\nCurrent configuration:")
    server_client.update_configuration(protocol_pb2.UpdateConfiguration.Setting.COMPRESSED_RESERVED_MEMORY_IN_BYTES,
                                       10000000)
    print(server_client.get_configuration())

    clean_up_tables(server_client, [], "drop")
