""" A script that computes which part of modelardbd's compressed storage format
    uses the most storage at both the logical database, table, and column level.
"""

import glob
import sqlite3
import sys
import tempfile
from os import path
from collections import defaultdict

from pyarrow import parquet


class ColumnMetadata:
    """Represents the metadata of a logical column."""

    def __init__(self, name, index, error_bound):
        self.name = name.upper()
        self.index = index
        self.error_bound = error_bound

    def __str__(self):
        return f"{self.name}(error bound {self.error_bound}%)"


def read_metadata_for_table(data_folder, table_name):
    """Read the metadata for a table from the SQLite metadata database."""
    metadata_database = path.join(data_folder, "metadata.sqlite3")
    connection = sqlite3.connect(metadata_database)
    cursor = connection.cursor()

    cursor.execute(
        (
            "SELECT column_name, column_index, error_bound "
            "FROM model_table_field_columns "
            f"WHERE table_name = '{table_name}'"
        )
    )

    table_metadata = {}
    for row in cursor:
        column_metadata = ColumnMetadata(row[0], row[1], row[2])
        table_metadata[column_metadata.index] = column_metadata

    cursor.close()
    connection.close()
    return table_metadata


def compute_size_of_table_in_bytes_when_stored_as_parquet(table):
    """Compute the size of table when stored as an Apache Parquet file."""
    temporary_file = tempfile.NamedTemporaryFile()

    # Zstandard is used for compression to more closely match modelardbd.
    parquet.write_table(table, temporary_file, compression="zstd")
    temporary_file.flush()
    return path.getsize(temporary_file.name)


def print_size_in_mib(name, size_in_bytes):
    """Format and print the size of the entity with name in mebibytes."""
    size_in_mib = size_in_bytes / 1024 / 1024
    print(f"  {name:15} {size_in_mib:10.5f} MiB")


def compute_aggregate_and_print_size(name, table, aggregates):
    """Compute the size of table, add it to aggregates, and then print it."""
    size_in_bytes = compute_size_of_table_in_bytes_when_stored_as_parquet(table)
    aggregates[name] += size_in_bytes
    print_size_in_mib(name, size_in_bytes)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not path.isdir(sys.argv[1]):
        print(f"usage: {sys.argv[0]} modelardbd-local-data-folder")
        exit(1)

    database_aggregates = defaultdict(int)
    for table_folder in glob.glob(path.join(sys.argv[1], "compressed", "*")):
        table_name = path.basename(table_folder)
        print(f"{table_name.upper()}:")

        table_metadata = read_metadata_for_table(sys.argv[1], table_name)

        table_aggregates = defaultdict(int)
        for column_folder in glob.glob(path.join(table_folder, "*")):
            column_index = int(path.basename(column_folder))
            print(f" {table_metadata[column_index]}:")

            # The column may be stored as multiple files, so to more closely
            # match modelardbd, the read table is sorted like modelardbd does.
            compressed_table = parquet.read_table(column_folder)
            compressed_sorted_table = compressed_table.sort_by(
                [("univariate_id", "ascending"), ("start_time", "ascending")]
            )
            compute_aggregate_and_print_size(
                "[all columns]", compressed_sorted_table, table_aggregates
            )

            for compressed_column_name in compressed_sorted_table.column_names:
                compressed_column = compressed_sorted_table.select(
                    [compressed_column_name]
                )
                compute_aggregate_and_print_size(
                    compressed_column_name, compressed_column, table_aggregates
                )
            print()

        print(" ALL COLUMNS:")
        for name, size_in_bytes in table_aggregates.items():
            print_size_in_mib(name, size_in_bytes)
            database_aggregates[name] += size_in_bytes
        print()

    print("ALL TABLES:")
    for name, size_in_bytes in database_aggregates.items():
        print_size_in_mib(name, size_in_bytes)
