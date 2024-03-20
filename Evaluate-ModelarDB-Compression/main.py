"""Script for evaluating ModelarDB with different data sets."""

import sys
import math
import time
import signal
import tempfile
import subprocess
import collections

import numpy
from pyarrow import parquet
from pyarrow import flight

# Configuration.
MODELARDB_REPOSITORY = "https://github.com/ModelarData/ModelarDB-RS.git"
UTILITIES_REPOSITORY = "https://github.com/ModelarData/Utilities.git"
TABLE_NAME = "evaluate"
STDOUT = subprocess.PIPE
STDERR = subprocess.PIPE


# Helper Functions.
def extract_repository_name(url):
    # The plus operator is used instead of an fstring as it was more readable.
    return url[url.rfind("/") + 1 : url.rfind(".")] + "/"


def git_clone(url):
    subprocess.run(["git", "clone", url], stdout=STDOUT, stderr=STDERR)


def cargo_build_release(modelardb_folder):
    process = subprocess.run(
        ["cargo", "build", "--release"],
        cwd=modelardb_folder,
        stdout=STDOUT,
        stderr=STDERR,
    )

    return process.returncode == 0


def start_modelardbd(modelardb_folder, data_folder):
    process = subprocess.Popen(
        ["target/release/modelardbd", data_folder],
        cwd=modelardb_folder,
        stdout=STDOUT,
        stderr=STDERR,
    )

    # Ensure process is fully started.
    while not b"Starting Apache Arrow Flight on" in process.stdout.readline():
        time.sleep(1)

    return process


def errors_occurred(output_stream):
    normalized = output_stream.lower()
    return b"error" in normalized or b"panicked" in normalized


def print_stream(output_stream):
    print()
    print(output_stream.decode("utf-8"))


def ingest_test_data(utilities_loader, test_data, error_bound_str):
    process = subprocess.run(
        [
            "python3",
            utilities_loader,
            "127.0.0.1:9999",
            TABLE_NAME,
            test_data,
            error_bound_str,
        ],
        stdout=STDOUT,
        stderr=STDERR,
    )

    if errors_occurred(process.stderr):
        print_stream(process.stderr)
        return True


def retrieve_schema(flight_client):
    flight_descriptor = flight.FlightDescriptor.for_path(TABLE_NAME)
    schema_result = flight_client.get_schema(flight_descriptor)
    return schema_result.schema


def retrieve_ingested_columns(flight_client, timestamp_column_name, field_column_name):
    ticket = flight.Ticket(
        (
            f"SELECT {timestamp_column_name}, {field_column_name} "
            f"FROM {TABLE_NAME} ORDER BY {timestamp_column_name}"
        )
    )
    reader = flight_client.do_get(ticket)
    return reader.read_all()


def compute_and_print_metrics(
    test_data_timestamp_column,
    test_data_field_column,
    decompressed_columns,
    error_bound,
):
    # Arrays make iteration simpler and float32 match ModelarDB's precision.
    test_data_timestamp_column = test_data_timestamp_column.to_numpy()
    test_data_field_column = test_data_field_column.to_numpy().astype(numpy.float32)
    decompressed_timestamp_column = decompressed_columns[0].to_numpy()
    decompressed_field_column = decompressed_columns[1].to_numpy()

    # Initialize variables for computing metrics.
    equal_values = 0

    sum_absolute_difference = 0.0
    sum_absolute_test_data_values = 0.0
    sum_actual_error_ratio_for_mape = 0.0

    max_actual_error = 0.0
    max_actual_error_test_data_value = 0.0
    max_actual_error_decompressed_value = 0.0

    # Initialize a Counter for the actual error of each decompressed value
    # rounded to the nearest integer so a simple histogram can be printed.
    ceiled_actual_error_counter = collections.Counter()

    # Indices of the data points with a value that exceeds the error bound.
    indices_of_values_above_error_bound = []

    # Indices of the data points with a value that has an undefined error.
    indices_of_values_with_undefined_error = []

    # The length of each pair of timestamp and value columns should always be
    # equal as this is required by both Apache Parquet files and Apache Arrow
    # RecordBatches, however, it is checked just to be absolutely sure it is.
    if len(test_data_timestamp_column) != len(decompressed_timestamp_column) \
       or len(test_data_field_column) != len(decompressed_field_column):
        print(
            (
                "ERROR: the length of the columns in the test data "
                f"({len(test_data_timestamp_column)}) and length the decompressed "
                f"columns ({len(decompressed_timestamp_column)}) are not equal."
            )
        )
        return

    # Compute metrics.
    for index in range(0, len(test_data_timestamp_column)):
        test_data_timestamp = test_data_timestamp_column[index]
        test_data_value = test_data_field_column[index]
        decompressed_timestamp = decompressed_timestamp_column[index]
        decompressed_value = decompressed_field_column[index]

        if test_data_timestamp != decompressed_timestamp:
            print(
                (
                    f"ERROR: at index {index}, the timestamp in the test data "
                    f"({test_data_timestamp}) and the decompressed timestamp "
                    f"({decompressed_timestamp}) are not equal."
                )
            )
            return

        if test_data_value == decompressed_value or \
           (math.isnan(test_data_value) and math.isnan(decompressed_value)):
            equal_values += 1
            difference = 0.0
            actual_error_ratio = 0.0
        else:
            difference = test_data_value - decompressed_value
            actual_error_ratio = abs(difference / test_data_value)

        actual_error = 100.0 * actual_error_ratio

        sum_absolute_difference += abs(difference)
        sum_absolute_test_data_values += abs(test_data_value)
        sum_actual_error_ratio_for_mape += actual_error_ratio

        if max_actual_error < actual_error:
            max_actual_error = actual_error
            max_actual_error_test_data_value = test_data_value
            max_actual_error_decompressed_value = decompressed_value

        # math.ceil() raises errors if it receives one of the special floats:
        # -inf (OverflowError), inf (OverflowError), and NaN (ValueError).
        try:
            ceiled_actual_error_counter[math.ceil(actual_error)] += 1
        except (OverflowError, ValueError):
            ceiled_actual_error_counter["UNDEFINED"] += 1
            indices_of_values_with_undefined_error.append(index)

        if actual_error > error_bound:
            indices_of_values_above_error_bound.append(index)

    # Compute and print the final result.
    print(f"- Total Number of Values: {len(decompressed_field_column)}")
    print(f"- Without Error: {100 * (equal_values / len(decompressed_field_column))}%")
    print(
        (
            "- Average Relative Error: "
            f"{100 * (sum_absolute_difference / sum_absolute_test_data_values)}%"
        )
    )
    print(
        (
            "- Mean Absolute Percentage Error: "
            f"{100.0 * (sum_actual_error_ratio_for_mape / len(decompressed_field_column))}%"
        )
    )
    print(
        (
            f"- Maximum Error: {max_actual_error}% due to {max_actual_error_test_data_value} "
            f"(test data) and {max_actual_error_decompressed_value} (decompressed)"
        )
    )
    print("- Error Ceil Histogram:", end="")
    for ceiled_error in range(0, math.ceil(max_actual_error) + 1):
        print(f" {ceiled_error}% {ceiled_actual_error_counter[ceiled_error]} ", end="")

    if ceiled_actual_error_counter['UNDEFINED'] != 0:
        print(f" Undefined {ceiled_actual_error_counter['UNDEFINED']}")
    else:
        print()

    print_data_points_if_any(
        "- Exceeded Error Bound (Timestamp, Test Data Value, Decompressed Value):",
        indices_of_values_above_error_bound, test_data_timestamp_column,
        test_data_field_column, decompressed_field_column)

    print_data_points_if_any(
        "- Undefined Actual Error (Timestamp, Test Data Value, Decompressed Value):",
        indices_of_values_with_undefined_error, test_data_timestamp_column,
        test_data_field_column, decompressed_field_column)
    print()


def print_data_points_if_any(header, indices, test_data_timestamp_column,
                             test_data_field_column, decompressed_field_column):
    if indices:
        print(header)

        for index in indices:
            print(
                (
                    f"  {test_data_timestamp_column[index]}, "
                    f"{test_data_field_column[index]: .10f}, "
                    f"{decompressed_field_column[index]: .10f}"
                )
            )


def measure_data_folder_size_in_kib(data_folder):
    du_output = subprocess.check_output(["du", "-k", "-d0", data_folder])
    return int(du_output.split(b"\t")[0])


def send_sigint_to_process(process):
    process.send_signal(signal.SIGINT)

    # Ensure process is fully shutdown.
    while process.poll() is None:
        time.sleep(1)

    process.wait()

    stderr = process.stderr.read()
    if errors_occurred(stderr):
        print_stream(stderr)
        return True


# Main Function.
if __name__ == "__main__":
    # Ensure the necessary arguments are provided.
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} test_data.parquet relative_error_bound*")
        sys.exit(1)

    # The script assumes it runs on Linux.
    if sys.platform != "linux":
        print(f"ERROR: {sys.argv[0]} only supports Linux")
        sys.exit(1)

    # Clone repositories.
    modelardb_folder = extract_repository_name(MODELARDB_REPOSITORY)
    git_clone(MODELARDB_REPOSITORY)

    utilities_folder = extract_repository_name(UTILITIES_REPOSITORY)
    utilities_loader = f"{utilities_folder}Apache-Parquet-Loader/main.py"
    git_clone(UTILITIES_REPOSITORY)

    # Prepare new executable.
    if not cargo_build_release(modelardb_folder):
        raise ValueError("Failed to build ModelarDB in release mode.")

    # Evaluate error bounds.
    flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
    for maybe_error_bound in sys.argv[2:]:
        # Prepare error bound.
        error_bound = float(maybe_error_bound)
        if error_bound < 0.0:
            raise ValueError("Error bound must be a positive normal float.")
        error_bound_str = maybe_error_bound

        delimiter = (13 + len(error_bound_str)) * "="
        print(delimiter)
        print(f"Error Bound: {error_bound_str}")
        print(delimiter)

        # Prepare data folder.
        temporary_directory = tempfile.TemporaryDirectory()
        data_folder = temporary_directory.name

        # Ingest the test data.
        modelardbd = start_modelardbd(modelardb_folder, data_folder)
        failed_ingest = ingest_test_data(utilities_loader, sys.argv[1], error_bound_str)
        failed_sigint = send_sigint_to_process(modelardbd)  # Flush.
        if failed_ingest or failed_sigint:
            raise ValueError("Failed to ingest test data.")

        # Read the test data so metrics can be computed.
        test_data = parquet.read_table(sys.argv[1])

        # Retrieve each field column, compute metrics for it, and print them.
        modelardbd = start_modelardbd(modelardb_folder, data_folder)
        schema = retrieve_schema(flight_client)
        timestamp_column_name = list(
            filter(lambda nc: nc[1] == "timestamp[ms]", zip(schema.names, schema.types))
        )[0][0]
        test_data_timestamp_column = test_data.column(timestamp_column_name)

        for column_name, column_type in zip(schema.names, schema.types):
            if column_type == "float":
                print(column_name)
                field_column_name = column_name

                try:
                    test_data_field_column = test_data.column(field_column_name)
                except:
                    if field_column_name.__contains__(" "):
                        # Spaces in the name may have been replaced by underscores.
                        field_column_name_with_space = field_column_name.replace("_", " ")
                        test_data_field_column = test_data.column(
                            field_column_name_with_space
                        )
                    else: 
                        # Camel case columns may have been converted to lower case.
                        test_data_cols = [i.lower() for i in test_data.column_names]
                        test_data_field_column = test_data.column(test_data_cols.index(field_column_name))
                
                decompressed_columns = retrieve_ingested_columns(
                    flight_client, timestamp_column_name, field_column_name
                )

                compute_and_print_metrics(
                    test_data_timestamp_column,
                    test_data_field_column,
                    decompressed_columns,
                    error_bound,
                )

        if send_sigint_to_process(modelardbd):
            raise ValueError("Failed to measure the size of the data folder.")

        size_of_data_folder = measure_data_folder_size_in_kib(data_folder)
        print(
            "Data Folder Size: {} KiB / {} MiB / {} GiB".format(
                size_of_data_folder,
                size_of_data_folder / 1024,
                size_of_data_folder / 1024 / 1024,
            )
        )

    flight_client.close()
