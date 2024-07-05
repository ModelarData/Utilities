import common

import time
from random import randrange

import pyarrow
from pyarrow import flight
from pyarrow._flight import Result, FlightClient, Ticket


# Helper functions.
def collect_metrics(flight_client: FlightClient) -> pyarrow.RecordBatch:
    action = flight.Action("CollectMetrics", "")
    response = flight_client.do_action(action)

    return response[0]


def get_configuration(flight_client: FlightClient) -> pyarrow.RecordBatch:
    action = flight.Action("GetConfiguration", "")
    response = flight_client.do_action(action)

    return response[0]


def update_configuration(flight_client: flight.FlightClient, setting: str, setting_value: str) -> list[Result]:
    encoded_setting = common.encode_argument(setting)
    encoded_setting_value = common.encode_argument(setting_value)

    action_body = encoded_setting + encoded_setting_value
    action = flight.Action("UpdateConfiguration", action_body)
    response = flight_client.do_action(action)

    return list(response)


def ingest_into_edge_and_query_table(flight_client: FlightClient, table_name: str, num_rows: int) -> None:
    """
    Ingest num_rows rows into the table, flush the memory of the edge, and query the first five rows of the table.
    """
    record_batch = create_record_batch(num_rows)

    print(f"Ingesting data into {table_name}...")
    common.do_put(flight_client, table_name, record_batch)

    print("\nFlushing memory of the edge...\n")
    common.do_action(flight_client, "FlushMemory", "")

    print(f"First five rows of {table_name}:")
    query = Ticket(f"SELECT * FROM {table_name} LIMIT 5")
    common.do_get(flight_client, query)


def create_record_batch(num_rows: int) -> pyarrow.RecordBatch:
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
    server_client = flight.FlightClient("grpc://127.0.0.1:9999")

    common.create_test_tables(server_client)
    ingest_into_edge_and_query_table(server_client, "test_model_table_1", 10000)
