import time
from random import randrange

import pandas as pd
import pyarrow
from pyarrow import flight
from pyarrow._flight import Result, Ticket

from common import ModelarDBFlightClient, encode_argument


class ModelarDBServerFlightClient(ModelarDBFlightClient):
    """Functionality for interacting with a ModelarDB server using Apache Arrow Flight."""

    def do_put(self, table_name: str, record_batch: pyarrow.RecordBatch) -> None:
        """Insert the data in the given record batch into the table with the given table name."""
        upload_descriptor = flight.FlightDescriptor.for_path(table_name)
        writer, _ = self.flight_client.do_put(upload_descriptor, record_batch.schema)

        writer.write(record_batch)
        writer.close()

    def collect_metrics(self) -> pd.DataFrame:
        """Collect metrics from the server and return them as a pandas DataFrame."""
        response = self.do_action("CollectMetrics", b"")

        batch_bytes = response[0].body.to_pybytes()
        metric_df = pyarrow.ipc.RecordBatchStreamReader(batch_bytes).read_pandas()

        return metric_df

    def get_configuration(self) -> pd.DataFrame:
        """Get the current configuration of the server and return it as a pandas DataFrame."""
        response = self.do_action("GetConfiguration", b"")

        batch_bytes = response[0].body.to_pybytes()
        configuration_df = pyarrow.ipc.RecordBatchStreamReader(batch_bytes).read_pandas()

        return configuration_df

    def update_configuration(self, setting: str, setting_value: str) -> list[Result]:
        """Update the given setting to the given setting value in the server configuration."""
        encoded_setting = encode_argument(setting)
        encoded_setting_value = encode_argument(setting_value)

        action_body = encoded_setting + encoded_setting_value
        return self.do_action("UpdateConfiguration", action_body)

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


def create_record_batch(num_rows: int) -> pyarrow.RecordBatch:
    """
    Create a record batch with num_rows rows of randomly generated data for a table with one timestamp column,
    three tag columns, and three field columns.
    """
    schema = pyarrow.schema(
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
        schema=schema,
    )


if __name__ == "__main__":
    server_client = ModelarDBServerFlightClient("grpc://127.0.0.1:9999")
    print(f"Node type: {server_client.node_type()}\n")

    server_client.create_test_tables()
    server_client.ingest_into_server_and_query_table("test_model_table_1", 10000)

    print("\nCurrent metrics:")
    print(server_client.collect_metrics().to_string())

    print("\nCurrent configuration:")
    server_client.update_configuration("compressed_reserved_memory_in_bytes", "10000000")
    print(server_client.get_configuration())
