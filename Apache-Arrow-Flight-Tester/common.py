from typing import Literal

from pyarrow import flight, Schema
from pyarrow._flight import FlightInfo, ActionType, Result


class ModelarDBFlightClient:
    """Common functionality for interacting with server and manager ModelarDB instances using Apache Arrow Flight."""

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

    def do_action(self, action_type: str, action_body: bytes) -> list[Result]:
        """Wrapper around the do_action method of the FlightClient class."""
        action = flight.Action(action_type, action_body)
        response = self.flight_client.do_action(action)

        return list(response)

    def list_actions(self) -> list[ActionType]:
        """Wrapper around the list_actions method of the FlightClient class."""
        response = self.flight_client.list_actions()

        return list(response)

    def list_table_names(self) -> list[str]:
        """Return the names of the tables in the server or manager."""
        flights = self.list_flights()
        return [table_name.decode("utf-8") for table_name in flights[0].descriptor.path]

    def create_table(self, table_name: str, columns: list[tuple[str, str]], model_table=False) -> None:
        """
        Create a table in the server or manager with the given name and columns. Each pair in columns should have the
        format (column_name, column_type).
        """
        create_table = "CREATE MODEL TABLE" if model_table else "CREATE TABLE"
        sql = f"{create_table} {table_name}({', '.join([f'{column[0]} {column[1]}' for column in columns])})"
        self.do_action("CreateTable", str.encode(sql))

    def create_test_tables(self) -> None:
        """
        Create a table and a model table using the flight client, print the current tables to ensure the created tables are
        included, and print the schema for the created table and model table to ensure the tables are created correctly.
        """
        print("Creating test tables...")

        self.create_table("test_table_1", [("timestamp", "TIMESTAMP"), ("values", "REAL"), ("metadata", "REAL")])
        self.create_table("test_model_table_1", [
            ("location", "TAG"), ("install_year", "TAG"), ("model", "TAG"),
            ("timestamp", "TIMESTAMP"), ("power_output", "FIELD"),
            ("wind_speed", "FIELD"), ("temperature", "FIELD(5%)")
        ], model_table=True)

        print("\nCurrent tables:")
        for table_name in self.list_table_names():
            print(f"{table_name}:")
            print(f"{self.get_schema(table_name)}\n")

    def drop_table(self, table_name: str) -> None:
        """Drop the table with the given name from the server or manager."""
        self.do_action("DropTable", str.encode(table_name))

    def truncate_table(self, table_name: str) -> None:
        """Truncate the table with the given name in the server or manager."""
        self.do_action("TruncateTable", str.encode(table_name))

    def clean_up_tables(self, tables: list[str], operation: Literal["drop", "truncate"]) -> None:
        """
        Clean up the given tables by either dropping them or truncating them. If no tables are given, all tables
        are dropped or truncated.
        """
        if len(tables) == 0:
            tables = self.list_table_names()

        print(f"Cleaning up {', '.join(tables)} using {operation}...")

        for table_name in tables:
            self.drop_table(table_name) if operation == "drop" else self.truncate_table(table_name)


def encode_argument(argument: str) -> bytes:
    """Encode the given argument as bytes and prepend the size of the argument as a 2-byte integer."""
    argument_bytes = str.encode(argument)
    argument_size = len(argument_bytes).to_bytes(2, byteorder="big")

    return argument_size + argument_bytes
