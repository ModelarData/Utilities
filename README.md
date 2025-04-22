# Utilities
Utilities for simplifying development and testing developed in this repository:

- [Apache Arrow Flight manager testing script](Apache-Arrow-Flight-Tester/manager.py) written in
  [Python 3](https://www.python.org/) to test the different endpoints of the ModelarDB manager Apache Arrow Flight API.

- [Apache Arrow Flight server testing script](Apache-Arrow-Flight-Tester/server.py) written in
  [Python 3](https://www.python.org/) to test the different endpoints of the ModelarDB server Apache Arrow Flight API.

- [Apache Parquet loading script](Apache-Parquet-Loader/main.py) written in [Python 3](https://www.python.org/) to read
  Apache Parquet files with equivalent schemas, create a model table with a matching schema if it does not exist, and
  load their data into the created table.
  
- [Git Hooks](Git-Hooks) written in different scripting languages to ensure that the state of a repository is correct
  before or after a specific action has been performed.
  
- [ModelarDB analyze storage script](ModelarDB-Analyze-Storage/main.py) written in [Python 3](https://www.python.org/) 
  to extract how [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) compresses each field stored in a model table.
  For a data folder and model table, the script reads the Apache Parquet files and computes which model types are used
  and how much space each column in the stored Apache Parquet files uses.

- [ModelarDB evaluate changes script](ModelarDB-Evaluate-Changes/main.py) written in
  [Python 3](https://www.python.org/) to evaluate what impact a set of changes has on 
  [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) in terms of ingestion time, query processing time, and the
  amount of storage required. The script automatically computes and evaluates all possible combinations for the set of
  changes. For more information see [ModelarDB evaluator changes README.md](ModelarDB-Evaluate-Changes/README.md).

- [ModelarDB validate compression script](ModelarDB-Validate-Compression/main.py) written in 
  [Python 3](https://www.python.org/) compresses data sets for a set of error bounds and validates that all values are
   within the error bound. For each error bound, the script ingests Apache Parquet files with the same schema and
   computes multiple metrics about how ModelarDB represents the ingested data set, e.g., the amount of space needed.  

## Other Tools
Utilities for simplifying development and testing developed by other developers:

- [DamascenoRafael/mqtt-simulator](https://github.com/DamascenoRafael/mqtt-simulator) written in 
  [Python 3](https://www.python.org/) to simulate the sending of packets from sensors or devices to a broker.

## License
Unless otherwise stated, the utilities in this repository are licensed under version 2.0 of the Apache License and a
copy of the license is stored in the repository.
