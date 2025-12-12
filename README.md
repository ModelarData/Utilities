# Utilities
Utilities for simplifying development and testing developed in this repository:

- [Apache Arrow Flight server testing](Apache-Arrow-Flight-Tester/server.py) is a script written in
  [Python 3](https://www.python.org/) to test the different endpoints of the ModelarDB server Apache Arrow Flight API.

- [Apache Parquet loading](Apache-Parquet-Loader/main.py) is a script written in [Python 3](https://www.python.org/) to
  read Apache Parquet files with equivalent schemas, create a time series table with a matching schema if it does not
  exist, and load their data into the created time series table.
  
- [Git Hooks](Git-Hooks) are scripts written in different languages to ensure that the state of a repository is correct
  before or after a specific action has been performed.
  
- [ModelarDB analyze storage script](ModelarDB-Analyze-Storage/main.py) is a script written in
  [Python 3](https://www.python.org/) to compute how [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) compresses
  each field stored in a time series table. For a data folder and time series table, the script reads the Apache Parquet
  files and computes which model types are used and how much space each column in the stored Apache Parquet files uses.

- [ModelarDB evaluate changes script](ModelarDB-Evaluate-Changes/main.py) is a script written in
  [Python 3](https://www.python.org/) to evaluate what impact a set of changes has on 
  [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) in terms of ingestion time, query processing time, and the
  amount of storage required. The script automatically computes and evaluates all possible combinations for the set of
  changes. For more information see [ModelarDB evaluator changes README.md](ModelarDB-Evaluate-Changes/README.md).

- [ModelarDB validate compression script](ModelarDB-Validate-Compression/main.py) is a script written in 
  [Python 3](https://www.python.org/) to compress a data set for a set of error bounds and validate that all values are
   within the error bounds and compute various metrics. For each error bound, the script ingests Apache Parquet files
   with the same schema and computes multiple metrics about how ModelarDB represents the ingested data set, e.g., the
   amount of space needed.

## Other Tools
Utilities for simplifying development and testing developed by other developers:

- [DamascenoRafael/mqtt-simulator](https://github.com/DamascenoRafael/mqtt-simulator) written in 
  [Python 3](https://www.python.org/) to simulate the sending of packets from sensors or devices to a broker.

## License
Unless otherwise stated, the utilities in this repository are licensed under version 2.0 of the Apache License and a
copy of the license is stored in the repository.
