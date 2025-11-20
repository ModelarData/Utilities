import time
from random import randrange

import pyarrow


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
