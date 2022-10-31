# Evaluate ModelarDB Changes
This script evaluates what impact a set of changes has on
[ModelarDB](https://github.com/ModelarData/ModelarDB-RS) in terms of ingestion
time, query processing time, and the amount of storage required. The script
automatically computes and evaluates all possible combinations for the set of
changes. If a large number of combinations have been evaluated it may be
beneficial to analyze the non-dominated set, e.g., computed using
[`pareto.py`](https://github.com/matthewjwoodruff/pareto.py), instead of the raw
output.

The changes must be given in a `changes.json` file with the following format:
```json
{
  "file/in/modelardb-rs/to/change.rs#first_line_to_remove-last_line_to_remove": {
    "first_line_to_add({})": ["first_input_to_test", "second_input_to_test"],
    "second_line_to_add({})": ["first_input_to_test", "second_input_to_test"]
  }
}
```

As a concrete example, the following `changes.json` was used to evaluate the
impact of different settings for
[`ArrowWriter`](https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html)
for commit
[`8c0d145`](https://github.com/ModelarData/ModelarDB-RS/tree/8c0d14531ef42b8f08bd4bcce928cace990cbd9c):

```json
{
  "server/src/storage/mod.rs#215-216": {
    ".set_encoding(datafusion::parquet::basic::Encoding::{})": ["PLAIN", "PLAIN_DICTIONARY", "RLE", "BIT_PACKED", "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY", "RLE_DICTIONARY", "BYTE_STREAM_SPLIT"],
    ".set_compression(datafusion::parquet::basic::Compression::{})": ["UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "BROTLI", "LZ4", "ZSTD"],
    ".set_dictionary_enabled({})": ["false", "true"],
    ".set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::{})": ["None", "Chunk", "Page"]
  }
}
```
