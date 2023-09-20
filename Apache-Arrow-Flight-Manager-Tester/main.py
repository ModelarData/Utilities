from pyarrow import flight
from pyarrow._flight import FlightInfo, FlightClient, ActionType


# PyArrow Functions.
def list_flights(flight_client: FlightClient) -> list[FlightInfo]:
    response = flight_client.list_flights()

    return list(response)


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


if __name__ == "__main__":
    manager_client = flight.FlightClient("grpc://127.0.0.1:8888")

    print(list_actions(manager_client))
    print(list_table_names(manager_client))
