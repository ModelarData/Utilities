# Utilities
Utilities for simplifying development and testing:

- [Apache Arrow Flight manager testing script](Apache-Arrow-Flight-Tester/manager.py) written in
  [Python 3](https://www.python.org/) to test the different endpoints of the ModelarDB manager Apache Arrow Flight API.

- [Apache Arrow Flight server testing script](Apache-Arrow-Flight-Tester/server.py) written in
  [Python 3](https://www.python.org/) to test the different endpoints of the ModelarDB server Apache Arrow Flight API.

- [Apache Parquet loading script](Apache-Parquet-Loader) written in [Python 3](https://www.python.org/) to read Apache 
  Parquet files with equivalent schemas, create a model table with a matching schema if it does not exist, and load 
  their data into the created table.

- [ModelarDB change evaluator script](Evaluate-ModelarDB-Changes) written in [Python 3](https://www.python.org/) to 
  evaluate what impact a set of changes has on [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) in terms of
  ingestion time, query processing time, and the amount of storage required. The script automatically computes and 
  evaluates all possible combinations for the set of changes.

- [ModelarDB compression evaluator script](Evaluate-ModelarDB-Compression) written in [Python 3](https://www.python.org/) 
  to evaluate how well [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) compresses a data set stored in Apache 
  Parquet files with the same schema for a set of error bounds. For each error bound the script automatically ingests 
  the data set and computes metrics that indicate how well ModelarDB compressed the data set.

- [Git Hooks](Git-Hooks) written in different scripting languages to ensure that the state of a repository is correct 
  before or after a specific action has been performed.

- [ModelarDB evaluator](ModelarDB-Evaluator) written in [Rust](https://www.rust-lang.org/) to evaluate how well
  [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) compresses a data set stored in Apache Parquet files with the 
  same schema for a set of error bounds. For each error bound the script automatically ingests the data set and computes 
  metrics that indicate how well ModelarDB compressed the data set.

- [MQTT simulator](MQTT-Simulator) written in [Python 3](https://www.python.org/) to simulate the sending of packets 
  from sensors or devices to a broker. The simulator was originally forked from
  [DamascenoRafael/mqtt-simulator](https://github.com/DamascenoRafael/mqtt-simulator).

- [ModelarDB compression profiler script](Profile-ModelarDB-Compression) written in [Python 3](https://www.python.org/) 
  to compute how much storage each part of [ModelarDB](https://github.com/ModelarData/ModelarDB-RS)'s compressed format 
  uses at the logical database, table, and column level when stored in Apache Parquet files ordered by `univariate_id` 
  and `start_time` and compressed using Zstandard.

## License
Unless otherwise stated, the utilities in this repository are licensed under version 2.0 of the Apache License and a 
copy of the license is stored in the repository.
