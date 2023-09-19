from pyarrow import flight


def do_action(flight_client, action_type, action_body_str):
    action_body = str.encode(action_body_str)
    action = flight.Action(action_type, action_body)
    response = flight_client.do_action(action)

    print(list(response))


def list_actions(flight_client):
    response = flight_client.list_actions()

    print(list(response))


if __name__ == "__main__":
    manager_client = flight.FlightClient("grpc://127.0.0.1:8888")
