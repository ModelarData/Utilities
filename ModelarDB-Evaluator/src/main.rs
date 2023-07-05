/* Copyright 2023 The ModelarData Utilities Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Evaluator for checking and testing the compression performed by ModelarDB.

use std::collections::HashMap;
use std::env::{self, Args};
use std::fs::{self, DirEntry, File};
use std::io::Error as IOError;
use std::num::ParseFloatError;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, TimestampMillisecondArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::ParquetError;

use client::Client;
use server::Server;

mod client;
mod server;

/// Name of the model table used to ingest the normalized uncompressed test data into.
const TABLE_NAME: &str = "evaluate";

/// Location of the ModelarDB repository to retrieve the source code for ModelarDB from.
const MODELARDB_REPOSITORY: &str = "https://github.com/ModelarData/ModelarDB-RS.git";

/// Integer value for representing [`f64::NEG_INFINITY`] as [`u64`].
const INTEGER_NEGATIVE_INFINITY: u64 = u64::MAX - 2;

/// Integer value for representing [`f64::INFINITY`] as [`u64`].
const INTEGER_POSITIVE_INFINITY: u64 = u64::MAX - 1;

/// Integer value for representing [`f64::NAN`] as [`u64`].
const INTEGER_NAN: u64 = u64::MAX;

/// Extract an [`array`](arrow::array::Array) from a
/// [`RecordBatch`](arrow::record_batch::RecordBatch) and cast it to the specified type:
#[macro_export]
macro_rules! array {
    ($batch:ident, $column:ident, $type:ident) => {
        $batch
            .column($column)
            .as_any()
            .downcast_ref::<$type>()
            .unwrap()
    };
    ($batch:ident, $column:literal, $type:ident) => {
        $batch
            .column($column)
            .as_any()
            .downcast_ref::<$type>()
            .unwrap()
    };
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut args = env::args();

    if args.len() < 3 {
        // The errors are consciously ignored as the client is terminating.
        let binary_path = env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap();
        Err(format!(
            "Usage: {} uncompressed_data error_bound*",
            binary_name.to_str().unwrap()
        ))?;
    }

    // Drop the path of the executable.
    args.next();

    // unwrap() is safe as args contains at least three elements.
    let uncompressed_data_path = PathBuf::from(&args.next().unwrap());
    let uncompressed_data_paths = if uncompressed_data_path.is_dir() {
        fs::read_dir(uncompressed_data_path)
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<DirEntry>, IOError>>()
            .map_err(|error| error.to_string())?
            .iter()
            .map(|dir_entry| dir_entry.path())
            .collect()
    } else {
        vec![uncompressed_data_path]
    };
    let error_bounds = parse_error_bounds(&mut args).map_err(|error| error.to_string())?;

    download_update_and_compile_modelardb().map_err(|error| error.to_string())?;

    for uncompressed_data_path in uncompressed_data_paths {
        // unwrap() is not safe, but the program should terminate if no file or folder is passed.
        println!(
            "Uncompressed Data: {:?}",
            uncompressed_data_path.file_name().unwrap()
        );

        // Read uncompressed data.
        let uncompressed_data = read_and_normalize_uncompressed_data(&uncompressed_data_path)
            .map_err(|error| error.to_string())?;

        // unwrap() is safe as a model table must contain a timestamp column.
        let schema = uncompressed_data.schema();
        let timestamp_column_index = index_of_timestamp_column(&schema).unwrap();
        let timestamp_column = schema.fields()[timestamp_column_index].name();

        for error_bound in &error_bounds {
            println!("Error Bound: {error_bound}");

            // Start server and client.
            let temp_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
            let _modelardbd = Server::new(temp_dir.path());
            let mut client = Client::new().await.map_err(|error| error.to_string())?;

            // Ingest uncompressed data.
            client
                .ingest_uncompressed_data(uncompressed_data.clone(), *error_bound)
                .await
                .map_err(|error| error.to_string())?;

            // Compare the uncompressed data and the decompressed data.
            for index in 0..schema.fields().len() {
                let field = schema.field(index);

                if *field.data_type() == DataType::Float32 {
                    println!("{} - {}", index, field.name());

                    let decompressed_data = client
                        .retrieve_decompressed_data(timestamp_column, field.name())
                        .await
                        .map_err(|error| error.to_string())?;

                    compute_and_print_metrics(
                        &array!(
                            uncompressed_data,
                            timestamp_column_index,
                            TimestampMillisecondArray
                        ),
                        &array!(uncompressed_data, index, Float32Array),
                        &array!(decompressed_data, 0, TimestampMillisecondArray),
                        &array!(decompressed_data, 1, Float32Array),
                        *error_bound,
                    );
                }
            }
        }

        // Formatting newline.
        println!();
    }

    Ok(())
}

/// Return the remaining elements in `args` as a [`Vec<f32>`]. Returns [`ParseFloatError`] if one of
/// the elements in `args` cannot be parsed to a [`f32`].
fn parse_error_bounds(args: &mut Args) -> Result<Vec<f32>, ParseFloatError> {
    let mut error_bounds = vec![];

    while let Some(error_bound_string) = args.next() {
        error_bounds.push(error_bound_string.parse::<f32>()?);
    }

    Ok(error_bounds)
}

/// Checkout the source code for the latest version of ModelarDB and compile it using the
/// dev-release profile.
fn download_update_and_compile_modelardb() -> Result<(), IOError> {
    Command::new("git")
        .arg("clone")
        .arg(MODELARDB_REPOSITORY)
        .output()?;

    Command::new("git")
        .args(["pull", "--rebase"])
        .arg(MODELARDB_REPOSITORY)
        .output()?;

    Command::new("cargo")
        .args(["build", "--profile", "dev-release"])
        .current_dir("ModelarDB-RS")
        .output()?;

    Ok(())
}

/// Read the Apache Parquet file at `uncompressed_data_path`, remove columns that contain NULL
/// values, and ensure that the types used for its columns are types supported by modelardbd.
/// Returns [`ParquetError`] if the file cannot be read or the data in it cannot normalized.
fn read_and_normalize_uncompressed_data(
    uncompressed_data_path: &Path,
) -> Result<RecordBatch, ParquetError> {
    let file = File::open(uncompressed_data_path)
        .map_err(|error| ParquetError::External(Box::new(error)))?;

    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = reader_builder.with_batch_size(usize::MAX).build()?;

    if let Some(maybe_uncompressed_data) = reader.next() {
        let uncompressed_data =
            maybe_uncompressed_data.map_err(|error| ParquetError::ArrowError(error.to_string()))?;

        normalize_uncompressed_data(uncompressed_data)
            .map_err(|error| ParquetError::ArrowError(error.to_string()))
    } else {
        Err(ParquetError::EOF(
            "Apache Parquet file with uncompressed data is empty.".to_owned(),
        ))
    }
}

/// Normalize `uncompressed_data` by removing columns that contain NULL values and casting columns
/// with types that are not supported by modelardbd to types that are supported by modelardbd.
/// Returns [`ParquetError`] if `uncompressed_data` cannot be normalized.
fn normalize_uncompressed_data(uncompressed_data: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let schema = uncompressed_data.schema();

    let mut fields = Vec::with_capacity(uncompressed_data.num_columns());
    let mut columns = Vec::with_capacity(uncompressed_data.num_columns());

    for (field, column) in schema.fields().iter().zip(uncompressed_data.columns()) {
        if column.null_count() > 0 {
            println!("Skipped {} as it contains null values.", field.name());
            continue;
        }

        let name = field.name().replace(" ", "_");

        let (normalized_data_type, normalized_column) = match field.data_type() {
            DataType::Timestamp(_, None) => (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                compute::cast(&column, &DataType::Timestamp(TimeUnit::Millisecond, None))?,
            ),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => (
                DataType::Float32,
                compute::cast(&column, &DataType::Float32)?,
            ),
            DataType::Utf8 | DataType::LargeUtf8 => {
                (DataType::Utf8, compute::cast(&column, &DataType::Utf8)?)
            }
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "Only Timestamp, Float, and String are supported, {data_type} is not."
            )))?,
        };

        fields.push(Field::new(name, normalized_data_type, false));
        columns.push(normalized_column);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

/// Return the index of the first [`Field`] in `schema` with the type [`DataType::Timestamp`] if one
/// exists, otherwise [`None`] is returned.
fn index_of_timestamp_column(schema: &Schema) -> Option<usize> {
    let fields = schema.fields();
    for index in 0..fields.len() {
        if *fields[index].data_type() == DataType::Timestamp(TimeUnit::Millisecond, None) {
            return Some(index);
        }
    }

    None
}

/// Compute and print how precisely `decompressed_timestamps` and `decompressed_values` represents
/// `uncompressed_timestamps` and `uncompressed_values`. The timestamp, uncompressed value, and
/// decompressed value is printed for each data point with a decompressed value that is outside the
/// `error_bound` or has an undefined error.
fn compute_and_print_metrics(
    uncompressed_timestamps: &TimestampMillisecondArray,
    uncompressed_values: &Float32Array,
    decompressed_timestamps: &TimestampMillisecondArray,
    decompressed_values: &Float32Array,
    error_bound: f32,
) {
    // Initialize variables for computing metrics.
    let mut equal_values = 0;

    let mut sum_absolute_difference = 0.0;
    let mut sum_absolute_uncompressed_values = 0.0;
    let mut sum_actual_error_ratio_for_mape = 0.0;

    let mut max_actual_error = 0.0;
    let mut max_actual_error_test_data_value = 0.0;
    let mut max_actual_error_decompressed_value = 0.0;

    // Initialize a Counter for the actual error of each decompressed value
    // rounded to the nearest integer so a simple histogram can be printed.
    let mut ceiled_actual_error_counter = HashMap::<u64, u64>::new();

    // Indices of the data points with a value that exceeds the error bound.
    let mut indices_of_values_above_error_bound = vec![];

    // Indices of the data points with a value that has an undefined error.
    let mut indices_of_values_with_undefined_error = vec![];

    // Ensure the number of rows in the uncompressed and decompressed data are equal, the number of
    // timestamps and values are required to be the same for Apache Parquet or Arrow RecordBatch'es.
    if uncompressed_timestamps.len() != decompressed_timestamps.len() {
        println!(
            "ERROR: the length of uncompressed ({}) and decompressed ({}) data are not equal.",
            uncompressed_timestamps.len(),
            decompressed_timestamps.len()
        );

        return;
    }

    // Compute metrics.
    for index in 0..decompressed_values.len() {
        let uncompressed_timestamp = uncompressed_timestamps.value(index);
        let uncompressed_value = uncompressed_values.value(index);
        let decompressed_timestamp = decompressed_timestamps.value(index);
        let decompressed_value = decompressed_values.value(index);

        if uncompressed_timestamp != decompressed_timestamp {
            println!(
                "ERROR: at index {}, the uncompressed timestamp ({}) is not equal to the decompressed \
                 timestamp ({}).", index, uncompressed_timestamp, decompressed_timestamp
            );

            return;
        }

        let (difference, actual_error_ratio) = if uncompressed_value == decompressed_value
            || (uncompressed_value.is_nan() && decompressed_value.is_nan())
        {
            equal_values += 1;
            (0.0, 0.0)
        } else {
            let difference = uncompressed_value - decompressed_value;
            (difference, f32::abs(difference / uncompressed_value))
        };

        let actual_error = 100.0 * actual_error_ratio;

        sum_absolute_difference += f32::abs(difference);
        sum_absolute_uncompressed_values += f32::abs(uncompressed_value);
        sum_actual_error_ratio_for_mape += actual_error_ratio;

        if max_actual_error < actual_error {
            max_actual_error = actual_error;
            max_actual_error_test_data_value = uncompressed_value;
            max_actual_error_decompressed_value = decompressed_value;
        }

        let ceiled_actual_error = actual_error.ceil();
        let integer_ceiled_actual_error = if ceiled_actual_error == f32::NEG_INFINITY {
            indices_of_values_with_undefined_error.push(index);
            INTEGER_NEGATIVE_INFINITY
        } else if ceiled_actual_error == f32::INFINITY {
            indices_of_values_with_undefined_error.push(index);
            INTEGER_POSITIVE_INFINITY
        } else if ceiled_actual_error.is_nan() {
            indices_of_values_with_undefined_error.push(index);
            INTEGER_NAN
        } else if ceiled_actual_error == ceiled_actual_error as u64 as f32 {
            ceiled_actual_error as u64
        } else {
            println!(
                "ERROR: the actual error {} cannot be represented as an integer.",
                ceiled_actual_error
            );

            return;
        };

        *ceiled_actual_error_counter
            .entry(integer_ceiled_actual_error)
            .or_insert(0) += 1;

        if actual_error > error_bound {
            indices_of_values_above_error_bound.push(index)
        }
    }

    // Compute and print the final result.
    println!("- Total Number of Values: {}", decompressed_values.len());
    println!(
        "- Without Error: {}%",
        100.0 * (equal_values as f64 / decompressed_values.len() as f64)
    );
    println!(
        "- Average Relative Error: {}%",
        100.0 * (sum_absolute_difference / sum_absolute_uncompressed_values)
    );
    println!(
        "- Mean Absolute Percentage Error: {}%",
        100.0 * (sum_actual_error_ratio_for_mape as f64 / decompressed_values.len() as f64)
    );
    println!(
        "- Maximum Error: {max_actual_error}% due to \
         {max_actual_error_test_data_value} (test data) and \
         {max_actual_error_decompressed_value} (decompressed)"
    );
    print!("- Error Ceil Histogram:");
    for (integer_ceiled_actual_error, count) in ceiled_actual_error_counter {
        let ceiled_actual_error = match integer_ceiled_actual_error {
            INTEGER_NEGATIVE_INFINITY => f32::NEG_INFINITY,
            INTEGER_POSITIVE_INFINITY => f32::INFINITY,
            INTEGER_NAN => f32::NAN,
            value => value as f32,
        };

        print!(" {ceiled_actual_error}% {count}")
    }
    println!();

    print_data_points_if_any(
        "- Exceeded Error Bound (Timestamp, Test Data Value, Decompressed Value):",
        &indices_of_values_above_error_bound,
        &uncompressed_timestamps,
        &uncompressed_values,
        &decompressed_values,
    );

    print_data_points_if_any(
        "- Undefined Actual Error (Timestamp, Test Data Value, Decompressed Value):",
        &indices_of_values_with_undefined_error,
        &uncompressed_timestamps,
        &uncompressed_values,
        &decompressed_values,
    );

    println!("");
}

/// Print a `header` followed by the timestamp, uncompressed value, and decompressed value for each
/// data point whose index is in `indicies`.
fn print_data_points_if_any(
    header: &str,
    indicies: &[usize],
    timestamps: &TimestampMillisecondArray,
    uncompressed_values: &Float32Array,
    decompressed_values: &Float32Array,
) {
    if !indicies.is_empty() {
        println!("{}", header);

        for index in indicies {
            println!(
                "  {} {} {}",
                timestamps.value(*index),
                uncompressed_values.value(*index),
                decompressed_values.value(*index)
            )
        }
    }
}
