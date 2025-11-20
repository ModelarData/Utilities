import time
from random import randrange

import pyarrow
from pyarrow import flight
from pyarrow._flight import Result, Ticket

from common import ModelarDBFlightClient, get_time_series_table_schema
from protobuf import protocol_pb2


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

    def ingest_into_server_and_query_table(self, table_name: str, num_rows: int) -> None:
        """
        Ingest num_rows rows into the table, flush the memory of the server, and query the first five rows of the table.
        """
        record_batch = create_record_batch(num_rows)

        print(f"Ingesting data into {table_name}...\n")
        self.do_put(table_name, record_batch)

        print("Flushing memory of the edge...\n")
        self.do_action("FlushMemory", b"")

        print(f"First five rows of {table_name}:")
        self.do_get(Ticket(f"SELECT * FROM {table_name} LIMIT 5"))

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


def create_record_batch(num_rows: int) -> pyarrow.RecordBatch:
    """
    Create a record batch with num_rows rows of randomly generated data for a table with one timestamp column,
    three tag columns, and three field columns.
    """
    schema = get_time_series_table_schema()

    location = ["aalborg" if i % 2 == 0 else "nibe" for i in range(num_rows)]
    install_year = ["2021" if i % 2 == 0 else "2022" for i in range(num_rows)]
    model = ["w72" if i % 2 == 0 else "w73" for i in range(num_rows)]

    timestamp = [round(time.time() * 1000000) + (i * 1000000) for i in range(num_rows)]
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
        schema=schema,
    )


if __name__ == "__main__":
    server_client = ModelarDBServerFlightClient("grpc://127.0.0.1:9999")
    print(f"Node type: {server_client.node_type()}\n")

    server_client.create_test_tables()
    server_client.ingest_into_server_and_query_table("test_time_series_table_1", 10000)

    print("\nCurrent configuration:")
    server_client.update_configuration(protocol_pb2.UpdateConfiguration.Setting.COMPRESSED_RESERVED_MEMORY_IN_BYTES,
                                       10000000)
    print(server_client.get_configuration())

    server_client.clean_up_tables([], "drop")
