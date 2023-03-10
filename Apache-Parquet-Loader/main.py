import sys
import pyarrow
from pyarrow import parquet
from pyarrow import flight


# Helper Functions.
def table_exists(flight_client, table_name):
    tables = map(lambda flight: flight.descriptor.path, flight_client.list_flights())
    return [bytes(table_name, "UTF-8")] in tables


def create_model_table(flight_client, table_name, schema, error_bound):
    # Construct the CREATE MODEL TABLE string.
    columns = []
    for field in schema:
        if field.type == pyarrow.timestamp("ms"):
            columns.append(field.name + " TIMESTAMP")
        elif field.type == pyarrow.float32():
            columns.append(field.name + " FIELD(" + error_bound + ")")
        elif field.type == pyarrow.string():
            columns.append(field.name + " TAG")
        else:
            raise ValueError("Unsupported Data Type: " + field.type)

    sql = "CREATE MODEL TABLE " + table_name + "(" + ", ".join(columns) + ")"

    # Execute the CREATE MODEL TABLE command.
    action = pyarrow.flight.Action("CommandStatementUpdate", str.encode(sql))
    result = flight_client.do_action(action)
    print(list(result))


def read_parquet_file_or_folder(path):
    # Read Apache Parquet file or folder.
    arrow_table = parquet.read_table(path)

    # Ensure the schema only uses supported features.
    columns = []
    column_names = []
    for field in arrow_table.schema:
        # Ensure that none of the field names contain whitespace.
        safe_name = field.name.replace(" ", "_")
        column_names.append(safe_name)

        # Ensure all fields are float32 as float64 are not supported.
        if field.type == pyarrow.float64():
            columns.append((safe_name, pyarrow.float32()))
        else:
            columns.append((safe_name, field.type))

    safe_schema = pyarrow.schema(columns)

    # Rename columns to remove whitespaces and cast them to remove float64.
    arrow_table = arrow_table.rename_columns(column_names)
    return arrow_table.cast(safe_schema)


def do_put_arrow_table(flight_client, table_name, arrow_table):
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, arrow_table.schema)
    writer.write(arrow_table)
    writer.close()


# Main Function.
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            "usage: "
            + sys.argv[0]
            + " address table parquet_file_or_folder [error_bound]"
        )
        sys.exit(1)

    flight_client = flight.FlightClient("grpc://" + sys.argv[1])
    table_name = sys.argv[2]
    arrow_table = read_parquet_file_or_folder(sys.argv[3])
    error_bound = sys.argv[4] if len(sys.argv) > 4 else 0.0

    if not table_exists(flight_client, table_name):
        create_model_table(flight_client, table_name, arrow_table.schema, error_bound)
    do_put_arrow_table(flight_client, table_name, arrow_table)
