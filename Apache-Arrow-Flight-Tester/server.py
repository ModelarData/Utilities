import pyarrow
from pyarrow import flight
from pyarrow._flight import Result, Ticket

import util
from wrapper import FlightClientWrapper
from protobuf import protocol_pb2


class ModelarDBServerFlightClient(FlightClientWrapper):
    """Functionality for interacting with a ModelarDB server using Apache Arrow Flight."""

    def list_table_names(self) -> list[str]:
        """Return the names of the tables in the server."""
        flights = self.list_flights()
        return [table_name.decode("utf-8") for table_name in flights[0].descriptor.path]

    def workload_balanced_query(self, query: str) -> None:
        """
        Retrieve a cloud node that can execute the given query and execute the query on the node. It is assumed that
        the cluster has at least one cloud node.
        """
        print("Retrieving cloud node that can execute the query...")
        query_descriptor = flight.FlightDescriptor.for_command(query)
        flight_info = self.flight_client.get_flight_info(query_descriptor)

        endpoint = flight_info.endpoints[0]
        cloud_node_url = endpoint.locations[0]

        print(f"Executing query on {cloud_node_url}...")
        cloud_client = ModelarDBServerFlightClient(cloud_node_url)
        cloud_client.do_get(endpoint.ticket)

    def create_table(self, table_name: str, columns: list[tuple[str, str]], time_series_table=False) -> None:
        """
        Create a table in the server with the given name and columns. Each pair in columns should have the
        format (column_name, column_type).
        """
        create_table = (
            "CREATE TIME SERIES TABLE" if time_series_table else "CREATE TABLE"
        )
        sql = f"{create_table} {table_name}({', '.join([f'{column[0]} {column[1]}' for column in columns])})"

        self.do_get(Ticket(sql))

    def drop_tables(self, table_names: list[str]) -> None:
        """Drop the given tables in the server."""
        self.do_get(Ticket(f"DROP TABLE {', '.join(table_names)}"))

    def truncate_tables(self, table_names: list[str]) -> None:
        """Truncate the given tables in the server."""
        self.do_get(Ticket(f"TRUNCATE {', '.join(table_names)}"))

    def vacuum_tables(self, table_names: list[str]) -> None:
        """Vacuum the given tables in the server."""
        self.do_get(Ticket(f"VACUUM {', '.join(table_names)}"))

    def create_normal_table_from_metadata(self, table_name: str, schema: pyarrow.Schema) -> None:
        """Create a normal table using the table name and schema."""
        normal_table_metadata = protocol_pb2.TableMetadata.NormalTableMetadata()

        normal_table_metadata.name = table_name
        normal_table_metadata.schema = schema.serialize().to_pybytes()

        table_metadata = protocol_pb2.TableMetadata()
        table_metadata.normal_table.CopyFrom(normal_table_metadata)

        self.do_action("CreateTable", table_metadata.SerializeToString())

    def create_time_series_table_from_metadata(self, table_name: str, schema: pyarrow.Schema, error_bounds: list[
        protocol_pb2.TableMetadata.TimeSeriesTableMetadata.ErrorBound], generated_columns: list[bytes]) -> None:
        """Create a time series table using the table name, schema, error bounds, and generated columns."""
        time_series_table_metadata = protocol_pb2.TableMetadata.TimeSeriesTableMetadata()

        time_series_table_metadata.name = table_name
        time_series_table_metadata.schema = schema.serialize().to_pybytes()
        time_series_table_metadata.error_bounds.extend(error_bounds)
        time_series_table_metadata.generated_column_expressions.extend(generated_columns)

        table_metadata = protocol_pb2.TableMetadata()
        table_metadata.time_series_table.CopyFrom(time_series_table_metadata)

        self.do_action("CreateTable", table_metadata.SerializeToString())

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

    def node_type(self) -> str:
        """Return the type of the node."""
        node_type = self.do_action("NodeType", b"")
        return node_type[0].body.to_pybytes().decode("utf-8")


if __name__ == "__main__":
    server_client = ModelarDBServerFlightClient("grpc://127.0.0.1:9999")
    print(f"Node type: {server_client.node_type()}\n")

    util.create_test_tables(server_client)
    util.ingest_into_server_and_query_table(server_client, "test_time_series_table_1", 10000)

    print("\nCurrent configuration:")
    server_client.update_configuration(protocol_pb2.UpdateConfiguration.Setting.COMPRESSED_RESERVED_MEMORY_IN_BYTES,
                                       10000000)
    print(server_client.get_configuration())

    util.clean_up_tables(server_client, [], "drop")
