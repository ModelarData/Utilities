import time
from random import randrange

import pyarrow
import pandas as pd
from pyarrow import flight


def create_table(client, table_name_str):
    schema = pyarrow.schema([
        ("timestamps", pyarrow.uint64()),
        ("values", pyarrow.float32()),
        ("metadata", pyarrow.float32()),
    ])

    table_name = str.encode(table_name_str)
    table_name_size = len(table_name).to_bytes(2, byteorder="big")

    schema_bytes = schema.serialize()
    schema_bytes_size = schema_bytes.size.to_bytes(2, byteorder="big")

    action_body = table_name_size + table_name + schema_bytes_size + schema_bytes

    result = client.do_action(pyarrow.flight.Action("CreateTable", action_body))
    print(list(result))


def create_model_table(client, table_name_str):
    table_name = str.encode(table_name_str)
    table_name_size = len(table_name).to_bytes(2, byteorder="big")

    schema_bytes = get_schema().serialize()
    schema_bytes_size = schema_bytes.size.to_bytes(2, byteorder="big")

    tag_indices = bytes([0, 1, 2])
    tag_indices_size = len(tag_indices).to_bytes(2, byteorder="big")

    timestamp_index = bytes([3])
    timestamp_index_size = len(timestamp_index).to_bytes(2, byteorder="big")

    action_body = (table_name_size + table_name + schema_bytes_size + schema_bytes + tag_indices_size + tag_indices
                   + timestamp_index_size + timestamp_index)

    result = client.do_action(pyarrow.flight.Action("CreateModelTable", action_body))
    print(list(result))


def list_actions(client):
    result = client.list_actions()
    print(list(result))


def insert_data(client, table_name):
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path(table_name)
    writer, _ = client.do_put(upload_descriptor, get_schema())

    record_batch = create_record_batch(10000)
    writer.write(record_batch)


def create_record_batch(num_rows):
    location = ["aalborg" if i % 2 == 0 else "nibe" for i in range(num_rows)]
    install_year = ["2021" if i % 2 == 0 else "2022" for i in range(num_rows)]
    model = ["w72" if i % 2 == 0 else "w73" for i in range(num_rows)]

    timestamp = [round(time.time() * 1000) + (i * 1000) for i in range(num_rows)]
    power_output = [float(randrange(0, 30)) for _ in range(num_rows)]
    wind_speed = [float(randrange(50, 100)) for _ in range(num_rows)]
    temperature = [float(randrange(0, 40)) for _ in range(num_rows)]

    df = pd.DataFrame({"location": location, "install_year": install_year, "model": model, "timestamp": timestamp,
                       "power_output": power_output, "wind_speed": wind_speed, "temperature": temperature})

    return pyarrow.RecordBatch.from_pandas(df=df, schema=get_schema())


def get_table_schema(client, table_name):
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path(table_name)
    response = client.get_schema(upload_descriptor)

    print(response.schema)


def get_schema():
    return pyarrow.schema([
        ("location", pyarrow.utf8()),
        ("install_year", pyarrow.utf8()),
        ("model", pyarrow.utf8()),
        ("timestamp", pyarrow.timestamp("ms")),
        ("power_output", pyarrow.float32()),
        ("wind_speed", pyarrow.float32()),
        ("temperature", pyarrow.float32()),
    ])


if __name__ == '__main__':
    flight_client = flight.FlightClient('grpc://127.0.0.1:9999')
    # create_table(flight_client, "test_table_1")
    # create_model_table(flight_client, "test_model_table_1")
    # list_actions(flight_client)
    # insert_data(flight_client, "test_model_table_1")
    # get_table_schema(flight_client, "test_table_1")
