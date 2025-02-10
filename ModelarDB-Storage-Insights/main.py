import os
import sys
import tempfile
from dataclasses import dataclass
from collections import Counter

import pyarrow
from pyarrow import parquet
from pyarrow import Table


MODEL_TYPE_ID_TO_NAME = ["PMC_Mean", "Swing", "Gorilla"]


@dataclass
class FileResult:
    file_path: str
    field_column: int
    model_types_used: dict[str, int]
    rust_size_in_bytes: int
    python_size_in_bytes: int
    python_size_in_bytes_per_column: dict[str, int]


def list_and_process_files(model_table_path: str) -> [FileResult]:
    file_results = []

    for dirpath, _dirnames, filenames in os.walk(model_table_path):
        for filename in filenames:
            if not filename.endswith(".parquet"):
                continue

            file_path = os.path.join(dirpath, filename)
            file_result = measure_file_and_its_columns(file_path)
            file_results.append(file_result)

    return file_results


def measure_file_and_its_columns(file_path: str) -> FileResult:
    table = parquet.read_table(file_path)

    field_column_str = file_path.split(os.sep)[-2]
    field_column = int(field_column_str[field_column_str.rfind("=") + 1 :])

    rust_size_in_bytes = os.path.getsize(file_path)
    python_size_in_bytes = write_table(table)

    model_types_used = Counter()
    python_size_in_bytes_per_column = Counter()
    for field in table.schema:
        column = table.column(field.name)
        if field.name == "model_type_id":
            for value in column:
                model_type_name = MODEL_TYPE_ID_TO_NAME[value.as_py()]
                model_types_used[model_type_name] += 1

        column_schema = pyarrow.schema(pyarrow.struct([field]))
        column_table = Table.from_arrays([column], schema=column_schema)
        python_size_in_bytes_per_column[field.name] = write_table(column_table)

    return FileResult(
        file_path,
        field_column,
        model_types_used,
        rust_size_in_bytes,
        python_size_in_bytes,
        python_size_in_bytes_per_column,
    )


def write_table(table: Table) -> int:
    with tempfile.NamedTemporaryFile() as temp_file_path:
        parquet.write_table(
            table,
            temp_file_path.name,
            data_page_size=16384,
            row_group_size=65536,
            column_encoding="PLAIN",
            compression="ZSTD",
            use_dictionary=False,
            write_statistics=False,
        )
        return os.path.getsize(temp_file_path.name)


def print_file_results(file_results: list[FileResult]):
    field_model_types_used = Counter()
    field_rust_size_in_bytes = 0
    field_python_size_in_bytes = 0
    field_size_in_bytes_per_column = Counter()

    total_model_types_used = Counter()
    total_rust_size_in_bytes = 0
    total_python_size_in_bytes = 0
    total_size_in_bytes_per_column = Counter()

    file_results.sort(key=lambda fr: fr.field_column)

    last_field_column = file_results[0].field_column
    for file_result in file_results:
        if last_field_column != file_result.field_column:
            print_total_size_in_bytes(
                last_field_column,
                field_model_types_used,
                field_rust_size_in_bytes,
                field_python_size_in_bytes,
                field_size_in_bytes_per_column,
            )
            field_model_types_used = Counter()
            field_rust_size_in_bytes = 0
            field_python_size_in_bytes = 0
            field_size_in_bytes_per_column = Counter()

        field_model_types_used.update(file_result.model_types_used)
        field_rust_size_in_bytes += file_result.rust_size_in_bytes
        field_python_size_in_bytes += file_result.python_size_in_bytes
        field_size_in_bytes_per_column.update(
               file_result.python_size_in_bytes_per_column
        )

        total_model_types_used.update(file_result.model_types_used)
        total_rust_size_in_bytes += file_result.rust_size_in_bytes
        total_python_size_in_bytes += file_result.python_size_in_bytes
        total_size_in_bytes_per_column.update(
               file_result.python_size_in_bytes_per_column
        )

        last_field_column = file_result.field_column

    print_total_size_in_bytes(
        last_field_column,
        field_model_types_used,
        field_rust_size_in_bytes,
        field_python_size_in_bytes,
        field_size_in_bytes_per_column,
    )

    print_total_size_in_bytes(
        "All",
        total_model_types_used,
        total_rust_size_in_bytes,
        total_python_size_in_bytes,
        total_size_in_bytes_per_column,
    )


def print_total_size_in_bytes(
    field_column: int | str,
    model_types_used: dict[str, int],
    rust_size_in_bytes: int,
    python_size_in_bytes: int,
    size_in_bytes_per_column: dict[str, int],
):
    print(f"Field Column: {field_column}")
    print("------------------------------------------")

    for model_type_name, count in model_types_used.items():
        print(f"- {model_type_name:<20} {count:>10} Segments")
    print("------------------------------------------")

    summed_size_in_bytes = 0
    for column, size in size_in_bytes_per_column.items():
        print(f"- {column:<25} {bytes_to_mib(size):>10} MiB")
        summed_size_in_bytes += size

    print("------------------------------------------")
    print(f"- Summed Size {bytes_to_mib(summed_size_in_bytes):>24} MiB")
    print(f"- Python Size {bytes_to_mib(python_size_in_bytes):>24} MiB")
    print(f"- Rust Size {bytes_to_mib(rust_size_in_bytes):>26} MiB")
    print()


def bytes_to_mib(size_in_bytes: int) -> int:
    return round(size_in_bytes / 1024 / 1024, 2)


def main():
    if len(sys.argv) != 3:
        print(f"python3 {__file__} data_folder table_name")
        return

    data_folder = sys.argv[1]
    model_table = sys.argv[2]

    # TODO: read the name of field columns when the issue is fixed.
    # Link to issue: https://github.com/apache/arrow/issues/45283
    model_table_path = data_folder + os.sep + "tables" + os.sep + model_table
    file_results = list_and_process_files(model_table_path=model_table_path)
    print_file_results(file_results)


if __name__ == "__main__":
    main()
