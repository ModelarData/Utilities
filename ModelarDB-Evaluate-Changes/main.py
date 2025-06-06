"""Script for evaluating multiple different changes to ModelarDB."""

import os
import sys
import json
import time
import atexit
import signal
import itertools
import tempfile
import subprocess

# Configuration.
MODELARDB_REPOSITORY = "https://github.com/ModelarData/ModelarDB-RS.git"
UTILITIES_REPOSITORY = "https://github.com/ModelarData/Utilities.git"
TABLE_NAME = "evaluate_changes"
STDOUT = subprocess.PIPE
STDERR = subprocess.PIPE


# Helper Functions.
def read_changes(modelardb_folder, changes_path):
    with open(changes_path) as changes_file:
        changes_file_content = json.load(changes_file)
        assert len(changes_file_content) == 1, "Multiple changes are not yet supported"

        for where, lines in changes_file_content.items():
            # Compute the existing lines to replace.
            (file_path, start_end) = where.split("#")
            full_file_path = modelardb_folder + file_path
            (start, end) = start_end.split("-")

            # Compute all of the permutations to test.
            changes = []
            for line, arguments in lines.items():
                changes.append(list(map(lambda c: line.format(c), arguments)))
            changes = list(itertools.product(*changes))

            # Return the full set of changes to test.
            return (full_file_path, int(start), int(end), changes)


def extract_repository_name(url):
    return url[url.rfind("/") + 1 : url.rfind(".")] + "/"


def git_clone(url):
    subprocess.run(["git", "clone", url], stdout=STDOUT, stderr=STDERR)


def git_reset(path):
    subprocess.run(["git", "-C", path, "reset", "--hard"], stdout=STDOUT, stderr=STDERR)


def replace_lines(path, start, end, new_lines):
    with open(path, "r") as f:
        lines = f.readlines()

    # enumerate() starts at zero and start and end is one or above.
    start -= 1
    end -= 1

    with open(path, "w") as f:
        for line_number, line in enumerate(lines):
            if line_number >= start and line_number < end:
                # Skip the lines to be deleted.
                continue
            elif line_number == end:
                # Write the lines to be added.
                for new_line in new_lines:
                    f.write(new_line + "\n")
            else:
                # Write the line to be kept.
                f.write(line)


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


def ingest_test_data(utilities_loader, test_data):
    start_time = time.time()
    process = subprocess.run(
        ["python3", utilities_loader, "127.0.0.1:9999", TABLE_NAME, test_data],
        stdout=STDOUT,
        stderr=STDERR,
    )

    if errors_occurred(process.stderr):
        print_stream(process.stderr)
        return None
    else:
        return time.time() - start_time


def execute_queries(queries):
    start_time = time.time()
    process = subprocess.run(
        ["target/release/modelardb", queries],
        cwd=modelardb_folder,
        stdout=STDOUT,
        stderr=STDERR,
    )

    if errors_occurred(process.stderr):
        print_stream(process.stderr)
        return None
    else:
        return time.time() - start_time


def measure_data_folder_size(data_folder):
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
        return None
    else:
        # Indicate no errors occurred.
        return True


def append_finished_result(
    output_file,
    current_change,
    changes,
    ingestion_time,
    query_execution_times,
    data_folder_size,
):
    results = {
        "changes": changes,
        "ingestion_time_in_seconds": ingestion_time,
        "data_folder_size_in_kib": data_folder_size,
    }
    results.update(query_execution_times)

    output_file.write('  "')
    output_file.write(str(current_change))
    output_file.write('": ')
    output_file.write(json.dumps(results))
    output_file.write(",\n")
    output_file.flush()


def print_separator(current_change, last_change):
    if current_change != last_change:
        print(100 * "=")


def finish_output_file_and_kill_process(output_file):
    # Finished the output file.
    if not output_file.closed:
        output_file.seek(output_file.tell() - 2, 0)
        output_file.write("\n}\n")
        output_file.close()

    # Kill leftover processes.
    subprocess.run(["pkill", "-9", "cargo"], stdout=STDOUT, stderr=STDERR)
    subprocess.run(["pkill", "-9", "rustc"], stdout=STDOUT, stderr=STDERR)


# Main Function.
if __name__ == "__main__":
    # Ensure the necessary arguments are provided.
    if len(sys.argv) < 5:
        print(
            "usage: "
            + sys.argv[0]
            + " output_file changes.json parquet_file_or_folder queries.sql+"
        )
        sys.exit(1)

    # The script assumes it runs on Linux.
    if sys.platform != "linux":
        print("ERROR: " + sys.argv[0] + " only supports Linux")
        sys.exit(1)

    # Clone repositories.
    modelardb_folder = extract_repository_name(MODELARDB_REPOSITORY)
    git_clone(MODELARDB_REPOSITORY)

    utilities_folder = extract_repository_name(UTILITIES_REPOSITORY)
    utilities_loader = utilities_folder + "Apache-Parquet-Loader/main.py"
    git_clone(UTILITIES_REPOSITORY)

    # Read changes.
    (file_path, start, end, changes) = read_changes(modelardb_folder, sys.argv[2])
    if not os.path.isfile(file_path):
        print("ERROR: the file to change does not exist.")
        sys.exit(1)

    if start <= 0 or end <= 0:
        print("ERROR: the value of start or end is not positive.")
        sys.exit(1)

    # Compute absolute paths.
    test_data = os.path.abspath(sys.argv[3])
    query_sets = list(map(lambda q: os.path.abspath(q), sys.argv[4:]))

    # Open output file.
    output_file = open(sys.argv[1], "w")
    output_file.write("{\n")

    # Cleanup on exit.
    atexit.register(finish_output_file_and_kill_process, output_file)
    signal.signal(signal.SIGTERM, lambda _signal_number, _frame: sys.exit(0))
    signal.signal(signal.SIGINT, lambda _signal_number, _frame: sys.exit(0))

    # Evaluate changes.
    last_change = len(changes)
    for index, changes in enumerate(changes):
        # Print what changes are being evaluated.
        current_change = index + 1
        print("Evaluating Permutation {} of {}".format(current_change, last_change))
        print(file_path, start, end)
        print("\n".join(changes))

        # Prepare data folder.
        temporary_directory = tempfile.TemporaryDirectory()
        data_folder = temporary_directory.name

        # Prepare and run new executable.
        git_reset(modelardb_folder)
        replace_lines(file_path, start, end, changes)
        if not cargo_build_release(modelardb_folder):
            print("ERROR: failed to compile ModelarDB.")
            print_separator(current_change, last_change)
            continue

        # Measure ingestion time in seconds.
        modelardbd = start_modelardbd(modelardb_folder, data_folder)
        ingestion_time = ingest_test_data(utilities_loader, test_data)
        if not ingestion_time:
            print("ERROR: failed to ingest test data.")
            print_separator(current_change, last_change)
            continue

        # Measure query time in seconds.
        query_execution_times = {}
        for query_set in query_sets:
            query_time = execute_queries(query_set)
            if not query_time:
                print(f"ERROR: failed to execute queries in {query_set}.")
                print_separator(current_change, last_change)
                continue

            query_set_name = os.path.basename(query_set)
            query_execution_name = f"{query_set_name}_in_seconds"
            query_execution_times[query_execution_name] = query_time

        # Ensure the process is gone.
        successfully_killed = send_sigint_to_process(modelardbd)
        if not successfully_killed:
            print("ERROR: failed to terminate process.")
            print_separator(current_change, last_change)
            continue

        # Measure size of data folder in kilobytes.
        data_folder_size = measure_data_folder_size(data_folder)
        append_finished_result(
            output_file,
            current_change,
            changes,
            ingestion_time,
            query_execution_times,
            data_folder_size,
        )
        temporary_directory.cleanup()

        # Print a separator between each evaluation.
        print_separator(current_change, last_change)
