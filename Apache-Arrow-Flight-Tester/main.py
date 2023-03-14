import time
import pprint
from random import randrange

import pyarrow
from pyarrow import flight


# Helper Functions.
def get_pyarrow_schema():
    return pyarrow.schema(
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


def create_record_batch(num_rows):
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
        schema=get_pyarrow_schema(),
    )


# PyArrow Functions.
def list_flights(flight_client):
    response = flight_client.list_flights()

    print(list(response))


def get_schema(flight_client, table_name):
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path(table_name)
    response = flight_client.get_schema(upload_descriptor)

    print(response.schema)


def do_get(flight_client, ticket):
    ticket = flight.Ticket(ticket)
    response = flight_client.do_get(ticket)

    for batch in response:
        pprint.pprint(batch.data.to_pydict())


def do_put(flight_client, table_name):
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, get_pyarrow_schema())

    record_batch = create_record_batch(10000)
    writer.write(record_batch)
    writer.close()


def do_action(flight_client, action_type, action_body_str):
    action_body = str.encode(action_body_str)
    action = pyarrow.flight.Action(action_type, action_body)
    response = flight_client.do_action(action)

    print(list(response))


def list_actions(flight_client):
    response = flight_client.list_actions()

    print(list(response))


def update_object_store_to_min_io(flight_client, region, bucket_name, access_key_id, secret_access_key):
    object_store_type_bytes = str.encode("minio")
    object_store_type_size = len(object_store_type_bytes).to_bytes(2, byteorder="big")

    region_bytes = str.encode(region)
    region_size = len(region_bytes).to_bytes(2, byteorder="big")

    bucket_name_bytes = str.encode(bucket_name)
    bucket_name_size = len(bucket_name_bytes).to_bytes(2, byteorder="big")

    access_key_id_bytes = str.encode(access_key_id)
    access_key_id_size = len(access_key_id_bytes).to_bytes(2, byteorder="big")

    secret_access_key_bytes = str.encode(secret_access_key)
    secret_access_key_size = len(secret_access_key_bytes).to_bytes(2, byteorder="big")

    action_body = (object_store_type_size + object_store_type_bytes + region_size + region_bytes + bucket_name_size +
                   bucket_name_bytes + access_key_id_size + access_key_id_bytes + secret_access_key_size +
                   secret_access_key_bytes)

    result = flight_client.do_action(pyarrow.flight.Action("UpdateRemoteObjectStore", action_body))
    print(list(result))


def update_object_store_to_azure_blob_storage():
    pass


def delete_object_store():
    pass


# Main Function.
if __name__ == "__main__":
    flight_client = flight.FlightClient("grpc://127.0.0.1:9999")

    list_actions(flight_client)
    do_action(
        flight_client,
        "CommandStatementUpdate",
        "CREATE TABLE test_table_1(timestamp TIMESTAMP, values REAL, metadata REAL)",
    )
    do_action(
        flight_client,
        "CommandStatementUpdate",
        "CREATE MODEL TABLE test_model_table_1(location TAG, install_year TAG, model"
        " TAG, timestamp TIMESTAMP, power_output FIELD, wind_speed FIELD, temperature"
        " FIELD(5))",
    )

    list_flights(flight_client)
    get_schema(flight_client, "test_table_1")
    get_schema(flight_client, "test_model_table_1")

    do_put(flight_client, "test_model_table_1")
    do_get(flight_client, "SELECT * FROM test_model_table_1 LIMIT 5")
