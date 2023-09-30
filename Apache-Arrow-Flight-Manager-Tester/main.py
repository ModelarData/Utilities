from pyarrow import flight, Schema
from pyarrow._flight import FlightInfo, FlightClient, ActionType


# PyArrow Functions.
def list_flights(flight_client: FlightClient) -> list[FlightInfo]:
    response = flight_client.list_flights()

    return list(response)


def get_schema(flight_client: FlightClient, table_name: str) -> Schema:
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    response = flight_client.get_schema(upload_descriptor)

    return response.schema


def do_action(flight_client: FlightClient, action_type: str, action_body: str) -> list[any]:
    action_body_bytes = str.encode(action_body)
    action = flight.Action(action_type, action_body_bytes)
    response = flight_client.do_action(action)

    return list(response)


def list_actions(flight_client: FlightClient) -> list[ActionType]:
    response = flight_client.list_actions()

    return list(response)


# Helper functions.
def list_table_names(flight_client: FlightClient) -> list[str]:
    flights = list_flights(flight_client)

    return [table_name.decode("utf-8") for table_name in flights[0].descriptor.path]


def initialize_database(flight_client: FlightClient, tables: list[str]) -> list[str]:
    result = do_action(flight_client, "InitializeDatabase", ",".join(tables))[0]
    decoded_result = result.body.to_pybytes().decode("utf-8")

    return decoded_result.split(";")


if __name__ == "__main__":
    manager_client = flight.FlightClient("grpc://127.0.0.1:8888")

    print(list_actions(manager_client))
    print(list_table_names(manager_client))

    print(get_schema(manager_client, "test_table_1"))
    print(get_schema(manager_client, "test_model_table_1"))

    print(initialize_database(manager_client, ["test_model_table_1", "test_table_1"]))
