""" Script for evaluating multiple different changes to ModelarDB. """


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


def cargo_build_release(modelardb_cargo_toml):
    process = subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--bin",
            "modelardbd",
            "--manifest-path=" + modelardb_cargo_toml,
        ],
        stdout=STDOUT,
        stderr=STDERR,
    )

    return process.returncode == 0


def start_modelardbd(modelardb_cargo_toml, data_folder):
    process = subprocess.Popen(
        [
            "cargo",
            "run",
            "--release",
            "--bin",
            "modelardbd",
            "--manifest-path=" + modelardb_cargo_toml,
            data_folder,
        ],
        stdout=STDOUT,
        stderr=STDERR,
    )

    time.sleep(10)  # Ensure process is fully started.
    return process


def ingest_test_data(utilities_loader, test_data):
    start_time = time.time()
    process = subprocess.run(
        ["python3", utilities_loader, "127.0.0.1:9999", "evaluate_changes", test_data],
        stdout=STDOUT,
        stderr=STDERR,
    )

    if b"error" in process.stderr.lower():
        return None
    else:
        return time.time() - start_time


def execute_queries(queries):
    start_time = time.time()
    process = subprocess.run(
        [
            "cargo",
            "run",
            "--release",
            "--bin",
            "modelardb",
            "--manifest-path=" + modelardb_cargo_toml,
            queries,
        ],
        stdout=STDOUT,
        stderr=STDERR,
    )

    if b"error" in process.stderr.lower():
        return None
    else:
        return time.time() - start_time


def measure_data_folder_size(data_folder):
    du_output = subprocess.check_output(["du", "-k", "-d0", data_folder])
    return int(du_output.split(b"\t")[0])


def send_sigint_to_process(process):
    os.kill(process.pid, signal.SIGINT)
    time.sleep(10)  # Ensure process is fully shutdown.


def append_finished_result(
    output_file, changes, ingestion_time, query_time, data_folder_size
):
    results = {
        "ingestion_time_in_seconds": ingestion_time,
        "query_time_in_seconds": query_time,
        "data_folder_size_in_kib": data_folder_size,
    }

    output_file.write('  "')
    output_file.write(" ".join(changes))
    output_file.write('": ')
    output_file.write(json.dumps(results))
    output_file.write(",\n")
    output_file.flush()


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
    if len(sys.argv) != 5:
        print(
            "usage: "
            + sys.argv[0]
            + " changes.json test_data.parquet queries.sql output_file"
        )
        sys.exit(1)

    # The script assumes it runs on Linux.
    if sys.platform != "linux":
        print("ERROR: " + sys.argv[0] + " only supports Linux")
        sys.exit(1)

    # Clone repositories.
    modelardb_folder = extract_repository_name(MODELARDB_REPOSITORY)
    modelardb_cargo_toml = modelardb_folder + "Cargo.toml"
    git_clone(MODELARDB_REPOSITORY)

    utilities_folder = extract_repository_name(UTILITIES_REPOSITORY)
    utilities_loader = utilities_folder + "Apache-Parquet-Loader/main.py"
    git_clone(UTILITIES_REPOSITORY)

    # Read changes.
    (file_path, start, end, changes) = read_changes(modelardb_folder, sys.argv[1])
    if not os.path.isfile(file_path):
        print("ERROR: file to change does not exist.")
        sys.exit(1)

    # Open output file.
    output_file = open(sys.argv[4], "w")
    output_file.write("{\n")

    # Cleanup on exit.
    atexit.register(finish_output_file_and_kill_process, output_file)
    signal.signal(signal.SIGTERM, lambda _signal_number, _frame: sys.exit(0))
    signal.signal(signal.SIGINT, lambda _signal_number, _frame: sys.exit(0))

    # Evaluate changes.
    changes_len = len(changes)
    for index, changes in enumerate(changes):
        # Print what changes are being evaluating.
        print("Evaluating Permutation {} of {}".format(index + 1, changes_len))
        print(file_path, start, end)
        print("\n".join(changes))

        # Prepare data folder.
        temporary_directory = tempfile.TemporaryDirectory()
        data_folder = temporary_directory.name

        # Prepare and run new executable.
        git_reset(modelardb_folder)
        replace_lines(file_path, start, end, changes)
        if not cargo_build_release(modelardb_cargo_toml):
            print("ERROR: failed to compile ModelarDB.\n")
            continue

        # Measure ingestion time in seconds.
        modelardbd = start_modelardbd(modelardb_cargo_toml, data_folder)
        ingestion_time = ingest_test_data(utilities_loader, sys.argv[2])
        send_sigint_to_process(modelardbd)  # Ensure the data is flushed.
        if not ingestion_time:
            print("ERROR: failed to ingest test data.\n")
            continue

        # Measure query time in seconds.
        modelardbd = start_modelardbd(modelardb_cargo_toml, data_folder)
        query_time = execute_queries(sys.argv[3])
        send_sigint_to_process(modelardbd)  # Process is no longer needed.
        if not query_time:
            print("ERROR: failed to execute queries.\n")
            continue

        # Measure size of data folder in kilobytes.
        data_folder_size = measure_data_folder_size(data_folder)
        append_finished_result(
            output_file, changes, ingestion_time, query_time, data_folder_size
        )
        temporary_directory.cleanup()
        print()
