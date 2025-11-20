import pyarrow
import pprint

from protobuf import protocol_pb2
from pyarrow import flight, Schema
from pyarrow._flight import FlightInfo, ActionType, Result, Ticket


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

    def do_get(self, ticket: Ticket) -> None:
        """Wrapper around the do_get method of the FlightClient class."""
        response = self.flight_client.do_get(ticket)

        for batch in response:
            pprint.pprint(batch.data.to_pydict())

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

    def create_table(self, table_name: str, columns: list[tuple[str, str]], time_series_table=False) -> None:
        """
        Create a table in the server or manager with the given name and columns. Each pair in columns should have the
        format (column_name, column_type).
        """
        create_table = (
            "CREATE TIME SERIES TABLE" if time_series_table else "CREATE TABLE"
        )
        sql = f"{create_table} {table_name}({', '.join([f'{column[0]} {column[1]}' for column in columns])})"

        self.do_get(Ticket(sql))

    def create_normal_table_from_metadata(self, table_name: str, schema: pyarrow.Schema) -> None:
        """Create a normal table using the table name and schema."""
        normal_table_metadata = protocol_pb2.TableMetadata.NormalTableMetadata()

        normal_table_metadata.name = table_name
        normal_table_metadata.schema = schema.serialize().to_pybytes()

        table_metadata = protocol_pb2.TableMetadata()
        table_metadata.normal_table.CopyFrom(normal_table_metadata)

        self.do_action("CreateTable", table_metadata.SerializeToString())

    def create_time_series_table_from_metadata(self, table_name: str, schema: pyarrow.Schema, error_bounds: list[
        protocol_pb2.TableMetadata.TimeSeriesTableMetadata.ErrorBound], generated_columns: list[bytes]) -> None:
        """Create a time series table using the table name, schema, error bounds, and generated columns."""
        time_series_table_metadata = protocol_pb2.TableMetadata.TimeSeriesTableMetadata()

        time_series_table_metadata.name = table_name
        time_series_table_metadata.schema = schema.serialize().to_pybytes()
        time_series_table_metadata.error_bounds.extend(error_bounds)
        time_series_table_metadata.generated_column_expressions.extend(generated_columns)

        table_metadata = protocol_pb2.TableMetadata()
        table_metadata.time_series_table.CopyFrom(time_series_table_metadata)

        self.do_action("CreateTable", table_metadata.SerializeToString())

    def drop_table(self, table_name: str) -> None:
        """Drop the table with the given name from the server or manager."""
        self.do_get(Ticket(f"DROP TABLE {table_name}"))

    def truncate_table(self, table_name: str) -> None:
        """Truncate the table with the given name in the server or manager."""
        self.do_get(Ticket(f"TRUNCATE TABLE {table_name}"))

    def vacuum(self, table_names: list[str]) -> None:
        """Vacuum the given tables in the server or manager."""
        self.do_get(Ticket(f"VACUUM {', '.join(table_names)}"))

    def node_type(self) -> str:
        """Return the type of the node."""
        node_type = self.do_action("NodeType", b"")
        return node_type[0].body.to_pybytes().decode("utf-8")
