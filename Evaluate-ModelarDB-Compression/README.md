# Evaluate ModelarDB Compression
This script evaluates how well
[ModelarDB](https://github.com/ModelarData/ModelarDB-RS) compresses data sets
for a set of error bounds. For each error bound, the script ingests a data set
stored in one or more Apache Parquet files with the same schema, computes
multiple metrics to determine how precisely ModelarDB represents the ingested
data set, and computes the size of the compressed data. Thus, by using this
script it is simple to validate if changes to ModelarDB improves the compression
in terms of precision or the amount of storage required for multiple data sets.

