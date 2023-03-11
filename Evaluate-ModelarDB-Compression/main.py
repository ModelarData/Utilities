"""Script for evaluating ModelarDB with different data sets."""

import sys
import math
import time
import signal
import tempfile
import subprocess

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
        time.sleep(10)

    return process


def errors_occurred(output_stream):
    normalized = output_stream.lower()
    return b"error" in normalized or b"panicked" in normalized


def print_stream(output_stream):
    print()
    print(output_stream.decode("utf-8"))


def ingest_test_data(utilities_loader, test_data, error_bound):
    process = subprocess.run(
        [
            "python3",
            utilities_loader,
            "127.0.0.1:9999",
            TABLE_NAME,
            test_data,
            error_bound,
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


def retrieve_ingested_column(flight_client, column_name, timestamp_column):
    ticket = flight.Ticket(
        f"SELECT {column_name} FROM {TABLE_NAME} ORDER BY {timestamp_column}"
    )
    reader = flight_client.do_get(ticket)
    return reader.read_all().column(column_name)


def compute_and_print_metrics(real_column, compressed_column, error_bound):
    # Arrays make iteration simpler and float32 match ModelarDB's precision.
    real_column = real_column.to_numpy().astype(numpy.float32)
    compressed_column = compressed_column.to_numpy()

    # Initialize variables for computing metrics.
    equal_values = 0
    compared_values = 0

    sum_difference = 0.0
    sum_real_values = 0.0

    max_actual_error = 0.0
    max_actual_error_real_value = 0.0
    max_actual_error_compressed_value = 0.0

    ceiled_error_counts = math.ceil(error_bound + 2) * [0]

    # Compute metrics.
    for real_value, compressed_value in zip(real_column, compressed_column):
        if real_value == compressed_value:
            equal_values += 1.0
            difference = 0.0
            actual_error = 0.0
        else:
            difference = real_value - compressed_value
            actual_error = abs(difference / real_value)

        compared_values += 1

        sum_difference += difference
        sum_real_values += real_value

        if max_actual_error < actual_error:
            max_actual_error = actual_error
            max_actual_error_real_value = real_value
            max_actual_error_compressed_value = compressed_value

        try:
            ceiled_error_counts[math.ceil(actual_error)] += 1
        except OverflowError:
            print(
                "ERROR: undefined error due to {} (real) and {} (compressed).".format(
                    real_value, compressed_value
                )
            )
            print()
            return

    # Compute and print the final result.
    print(f"- Real Values: {len(real_column)}")
    print(f"- Compressed Values: {len(compressed_column)}")
    print(f"- Without Error: {100 * (equal_values / compared_values)}%")
    print(f"- Average Error: {100 * abs(sum_difference / sum_real_values)}%")
    print(
        f"- Maximum Error: {max_actual_error}% due to {max_actual_error_real_value} (real) and {max_actual_error_compressed_value} (compressed)"
    )
    print("- Error Ceil Histogram:", end="")
    for ceiled_error_count, count in enumerate(ceiled_error_counts):
        print(f" {ceiled_error_count}% {count} ", end="")
    print("\n")


def measure_data_folder_size_in_kib(data_folder):
    du_output = subprocess.check_output(["du", "-k", "-d0", data_folder])
    return int(du_output.split(b"\t")[0])


def send_sigint_to_process(process):
    process.send_signal(signal.SIGINT)

    # Ensure process is fully shutdown.
    while process.poll() is None:
        time.sleep(10)

    process.wait()

    stderr = process.stderr.read()
    if errors_occurred(stderr):
        print_stream(stderr)
        return True


# Main Function.
if __name__ == "__main__":
    # Ensure the necessary arguments are provided.
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} test_data.parquet error_bound*")
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
        error_bound_float = float(maybe_error_bound)
        if error_bound_float < 0.0:
            raise ValueError("Error bound must be positive.")
        error_bound = maybe_error_bound

        delimiter = (13 + len(error_bound)) * "="
        print(delimiter)
        print(f"Error Bound: {error_bound}")
        print(delimiter)

        # Prepare data folder.
        temporary_directory = tempfile.TemporaryDirectory()
        data_folder = temporary_directory.name

        # Ingest the test data.
        modelardbd = start_modelardbd(modelardb_folder, data_folder)
        failed_ingest = ingest_test_data(utilities_loader, sys.argv[1], error_bound)
        failed_sigint = send_sigint_to_process(modelardbd)  # Flush.
        if failed_ingest or failed_sigint:
            raise ValueError("Failed to ingest test data.")

        # Read the test data so metrics can be computed.
        test_data = parquet.read_table(sys.argv[1])

        # Retrieve each column, compute metrics for it, and print them.
        modelardbd = start_modelardbd(modelardb_folder, data_folder)
        schema = retrieve_schema(flight_client)
        timestamp_column = list(
            filter(lambda nc: nc[1] == "timestamp[ms]", zip(schema.names, schema.types))
        )[0][0]

        for column_name, column_type in zip(schema.names, schema.types):
            if column_type == "float":
                print(column_name)
                try:
                    real_column = test_data.column(column_name)
                except:
                    # Spaces in the name may have been replaced by underscores.
                    space_column_name = column_name.replace("_", " ")
                    real_column = test_data.column(space_column_name)
                compressed_column = retrieve_ingested_column(
                    flight_client, column_name, timestamp_column
                )
                compute_and_print_metrics(
                    real_column, compressed_column, error_bound_float
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
