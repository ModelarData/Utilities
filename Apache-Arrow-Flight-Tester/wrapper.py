import pyarrow
import pprint

from pyarrow import flight, Schema
from pyarrow._flight import FlightInfo, ActionType, Result, Ticket


class FlightClientWrapper:
    """Wrapper around the FlightClient class to simplify interaction with an Apache Arrow Flight server."""

    def __init__(self, location: str):
        self.flight_client = flight.FlightClient(location)

    def list_flights(self) -> list[FlightInfo]:
        """Wrapper around the list_flights method of the FlightClient class."""
        response = self.flight_client.list_flights()

        return list(response)

    def get_schema(self, table_name: str) -> Schema:
        """Wrapper around the get_schema method of the FlightClient class."""
        upload_descriptor = flight.FlightDescriptor.for_path(table_name)
        response = self.flight_client.get_schema(upload_descriptor)

        return response.schema

    def do_get(self, ticket: Ticket) -> None:
        """Wrapper around the do_get method of the FlightClient class."""
        response = self.flight_client.do_get(ticket)

        for batch in response:
            pprint.pprint(batch.data.to_pydict())

    def do_put(self, table_name: str, record_batch: pyarrow.RecordBatch) -> None:
        """Wrapper around the do_put method of the FlightClient class."""
        upload_descriptor = flight.FlightDescriptor.for_path(table_name)
        writer, _ = self.flight_client.do_put(upload_descriptor, record_batch.schema)

        writer.write(record_batch)
        writer.close()

    def do_action(self, action_type: str, action_body: bytes) -> list[Result]:
        """Wrapper around the do_action method of the FlightClient class."""
        action = flight.Action(action_type, action_body)
        response = self.flight_client.do_action(action)

        return list(response)

    def list_actions(self) -> list[ActionType]:
        """Wrapper around the list_actions method of the FlightClient class."""
        response = self.flight_client.list_actions()

        return list(response)
