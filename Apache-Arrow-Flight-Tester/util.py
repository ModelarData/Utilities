import time
from random import randrange
from typing import Literal

import pyarrow
from protobuf import protocol_pb2
from pyarrow._flight import Ticket

from server import ModelarDBServerFlightClient


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


def get_time_series_table_schema() -> pyarrow.Schema:
    """Return a schema for a time series table with one timestamp column, three tag columns, and three field columns."""
    return pyarrow.schema([
        ("location", pyarrow.utf8()),
        ("install_year", pyarrow.utf8()),
        ("model", pyarrow.utf8()),
        ("timestamp", pyarrow.timestamp("us")),
        ("power_output", pyarrow.float32()),
        ("wind_speed", pyarrow.float32()),
        ("temperature", pyarrow.float32()),
    ])


def create_test_tables(server_client: ModelarDBServerFlightClient) -> None:
    """
    Create a normal table and a time series table using the flight client, print the current tables to ensure the
    created tables are included, and print the schema to ensure the tables are created correctly.
    """
    print("Creating test tables...")

    server_client.create_table(
        "test_table_1",
        [("timestamp", "TIMESTAMP"), ("values", "REAL"), ("metadata", "REAL")],
    )
    server_client.create_table(
        "test_time_series_table_1",
        [
            ("location", "TAG"),
            ("install_year", "TAG"),
            ("model", "TAG"),
            ("timestamp", "TIMESTAMP"),
            ("power_output", "FIELD"),
            ("wind_speed", "FIELD"),
            ("temperature", "FIELD(5%)"),
        ],
        time_series_table=True,
    )

    print("\nCurrent tables:")
    for table_name in server_client.list_table_names():
        print(f"{table_name}:")
        print(f"{server_client.get_schema(table_name)}\n")


def create_test_tables_from_metadata(server_client: ModelarDBServerFlightClient):
    """
    Create a normal table and a time series table using the CreateTable action, print the current tables to ensure
    the created tables are included, and print the schema to ensure the tables are created correctly.
    """
    print("Creating test tables from metadata...")

    normal_table_schema = pyarrow.schema([
        ("timestamp", pyarrow.timestamp("us")),
        ("values", pyarrow.float32()),
        ("metadata", pyarrow.utf8())
    ])

    server_client.create_normal_table_from_metadata("test_table_1", normal_table_schema)

    time_series_table_schema = get_time_series_table_schema()

    lossless = protocol_pb2.TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type.LOSSLESS
    error_bounds = [protocol_pb2.TableMetadata.TimeSeriesTableMetadata.ErrorBound(value=0, type=lossless)
                    for _ in range(len(time_series_table_schema))]

    generated_column_expressions = [b'' for _ in range(len(time_series_table_schema))]

    server_client.create_time_series_table_from_metadata("test_time_series_table_1", time_series_table_schema,
                                                         error_bounds, generated_column_expressions)

    print("\nCurrent tables:")
    for table_name in server_client.list_table_names():
        print(f"{table_name}:")
        print(f"{server_client.get_schema(table_name)}\n")


def ingest_into_server_and_query_table(server_client: ModelarDBServerFlightClient, table_name: str,
                                       num_rows: int) -> None:
    """
    Ingest num_rows rows into the table, flush the memory of the server, and query the first five rows of the table.
    """
    record_batch = create_record_batch(num_rows)

    print(f"Ingesting data into {table_name}...\n")
    server_client.do_put(table_name, record_batch)

    print("Flushing memory of the edge...\n")
    server_client.do_action("FlushMemory", b"")

    print(f"First five rows of {table_name}:")
    server_client.do_get(Ticket(f"SELECT * FROM {table_name} LIMIT 5"))


def clean_up_tables(server_client: ModelarDBServerFlightClient, tables: list[str],
                    operation: Literal["drop", "truncate"]) -> None:
    """
    Clean up the given tables by either dropping them or truncating them. If no tables are given, all tables
    are dropped or truncated.
    """
    if len(tables) == 0:
        tables = server_client.list_table_names()

    print(f"Cleaning up {', '.join(tables)} using {operation}...")

    if operation == "drop":
        server_client.drop_tables(tables)
    else:
        server_client.truncate_tables(tables)
