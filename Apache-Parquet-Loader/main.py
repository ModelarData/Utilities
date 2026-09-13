import glob
import os
import sys

import pyarrow
from pyarrow import compute, flight, parquet


# Helper Functions.
def table_exists(flight_client, table_name):
    tables = (flight.descriptor.path for flight in flight_client.list_flights())
    return [bytes(table_name, "UTF-8")] in tables


def read_parquet_file_or_folder(path):
    arrow_table = parquet.read_table(path)

    # Ensure the schema only uses supported types.
    arrays = []
    fields = []

    for field in arrow_table.schema:
        column = arrow_table[field.name]

        if field.type in [pyarrow.timestamp("s"), pyarrow.timestamp("ms"),
                          pyarrow.timestamp("ns"), pyarrow.timestamp("us")]:
            # Ensure timestamps are timestamp[us] as others are not supported by modelardbd.
            column = compute.cast(column, pyarrow.timestamp("us"))
            fields.append(pyarrow.field(field.name, pyarrow.timestamp("us")))
        elif field.type in [pyarrow.float16(), pyarrow.float32(), pyarrow.float64()]:
            # Ensure fields are float32 as others are not supported by modelardbd.
            column = compute.cast(column, pyarrow.float32())
            fields.append(pyarrow.field(field.name, pyarrow.float32()))
        elif field.type in [pyarrow.string(), pyarrow.large_string(), pyarrow.string_view()]:
            # Ensure tags are strings as others are not supported by this loader.
            column = compute.cast(column, pyarrow.string())
            fields.append(pyarrow.field(field.name, pyarrow.string()))
        else:
            raise ValueError(f"Unsupported Data Type: {field.type}")

        arrays.append(column)

    # Create a new table with the supported types.
    return pyarrow.Table.from_arrays(arrays, schema=pyarrow.schema(fields))


def create_normal_table_sql(table_name, schema):
    # Construct the CREATE TABLE string with column names to also support
    # special characters in column names such as spaces and punctuation.
    # https://datafusion.apache.org/user-guide/sql/data_types.html
    columns = []
    for field in schema:
        if field.type == pyarrow.timestamp("us"):
            columns.append(f"`{field.name}` TIMESTAMP")
        elif field.type == pyarrow.float32():
            columns.append(f"`{field.name}` REAL")
        elif field.type == pyarrow.string():
            columns.append(f"`{field.name}` TEXT")
        else:
            raise ValueError(f"Unsupported Data Type: {field.type}")

    return f"CREATE TABLE {table_name} ({', '.join(columns)})"


def create_time_series_table_sql(table_name, schema, error_bound):
    # Construct the CREATE TIME SERIES TABLE string with column names
    # quoted to also support special characters in column names such
    # as spaces and punctuation.
    columns = []
    for field in schema:
        if field.type == pyarrow.timestamp("us"):
            columns.append(f"`{field.name}` TIMESTAMP")
        elif field.type == pyarrow.float32():
            columns.append(f"`{field.name}` FIELD({error_bound}%)")
        elif field.type == pyarrow.string():
            columns.append(f"`{field.name}` TAG")
        else:
            # This should never trigger as read_parquet_file_or_folder()
            # normalizes the schema of Apache Parquet files, but it is kept
            # to simplify debugging during development of the script itself.
            raise ValueError(f"Unsupported Data Type: {field.type}")

    return f"CREATE TIME SERIES TABLE {table_name} ({', '.join(columns)})"


def create_table(flight_client, sql):
    ticket = flight.Ticket(str.encode(sql))
    result = flight_client.do_get(ticket)
    return list(result)


def do_put_arrow_table(flight_client, table_name, arrow_table):
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, arrow_table.schema)
    writer.write(arrow_table)
    writer.close()


# Main Function.
if __name__ == "__main__":
    if len(sys.argv) != 5 and len(sys.argv) != 6:
        print(
            f"usage: {sys.argv[0]} host table_type table_name parquet_file_or_folder [relative_error_bound]"
        )
        sys.exit(1)

    flight_client = flight.FlightClient(f"grpc://{sys.argv[1]}")
    table_type = sys.argv[2]
    table_name = sys.argv[3]
    parquet_path = sys.argv[4]
    error_bound = sys.argv[5] if len(sys.argv) == 6 else "0.0"

    if os.path.isdir(parquet_path):
        parquet_files = glob.glob(parquet_path + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(parquet_path):
        parquet_files = [parquet_path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")

    # Assumes all of the files in the folder uses the same schema.
    arrow_table = read_parquet_file_or_folder(parquet_files[0])
    if not table_exists(flight_client, table_name):
        match table_type:
            case "normal":
                sql = create_normal_table_sql(table_name, arrow_table.schema)
                create_table(flight_client, sql)
            case "time_series":
                sql = create_time_series_table_sql(
                    table_name, arrow_table.schema, error_bound
                )
                create_table(flight_client, sql)
            case _:
                raise ValueError("table_type is not normal or time_series")

    for index, parquet_file in enumerate(parquet_files):
        print(f"- Processing {parquet_file} ({index + 1} of {len(parquet_files)})")
        arrow_table = read_parquet_file_or_folder(parquet_file)
        do_put_arrow_table(flight_client, table_name, arrow_table)

    # Flush the data to disk.
    action = flight.Action("FlushMemory", b"")
    result = flight_client.do_action(action)
    print(list(result))
