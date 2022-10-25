import time
from random import randrange

import pyarrow
import pandas as pd
from pyarrow import flight


# Helper Functions.
def get_pyarrow_schema():
    return pyarrow.schema([
        ("location", pyarrow.utf8()),
        ("install_year", pyarrow.utf8()),
        ("model", pyarrow.utf8()),
        ("timestamp", pyarrow.timestamp("ms")),
        ("power_output", pyarrow.float32()),
        ("wind_speed", pyarrow.float32()),
        ("temperature", pyarrow.float32()),
    ])


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

    return pyarrow.RecordBatch.from_pandas(df=df, schema=get_pyarrow_schema())


# PyArrow Function.
def list_flights(flight_client):
    response = flight_client.list_flights()

    print(response)


def get_schema(flight_client, table_name):
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path(table_name)
    response = flight_client.get_schema(upload_descriptor)

    print(response.schema)


def do_get(flight_client, table_name):
    ticket = flight.Ticket("SELECT * FROM " + table_name + " LIMIT 5")
    response = flight_client.do_get(ticket)

    for batch in response:
        print(batch)


def do_put(flight_client, table_name):
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, get_pyarrow_schema())

    record_batch = create_record_batch(10000)
    writer.write(record_batch)


def do_action(flight_client, action_type, action_body_str):
    action_body = str.encode(action_body_str)
    action = pyarrow.flight.Action(action_type, action_body)
    result = flight_client.do_action(action)

    print(list(result))


def list_actions(flight_client):
    result = flight_client.list_actions()

    print(list(result))


# Main Function.
if __name__ == '__main__':
    flight_client = flight.FlightClient('grpc://127.0.0.1:9999')

    list_actions(flight_client)
    do_action(flight_client, "CommandStatementUpdate", "CREATE TABLE test_table_1(timestamp TIMESTAMP, values REAL, metadata REAL)")
    do_action(flight_client, "CommandStatementUpdate", "CREATE MODEL TABLE test_model_table_1(location TAG, install_year TAG, model TAG, timestamp TIMESTAMP, power_output FIELD, wind_speed FIELD, temperature FIELD(5))")

    list_flights(flight_client)
    get_schema(flight_client, "test_table_1")
    get_schema(flight_client, "test_model_table_1")

    do_put(flight_client, "test_model_table_1")
    do_get(flight_client, "test_model_table_1")
