# Utilities
Utilities for simplifying development and testing:

- [Apache Arrow Flight testing script](Apache-Arrow-Flight-Tester) written in
  [Python 3](https://www.python.org/) to test the different endpoints of the
  ModelarDB Apache Arrow Flight API.

- [Apache Parquet loading script](Apache-Parquet-Loader) written in [Python
  3](https://www.python.org/) to read Apache Parquet files with equivalent
  schemas, create a model table with a matching schema if it does not exists,
  and load their data into the created table.

- [MQTT simulator](MQTT-Simulator) written in [Python
  3](https://www.python.org/) to simulate the sending of packets from sensors or
  devices to a broker. The simulator was originally forked from
  [DamascenoRafael/mqtt-simulator](https://github.com/DamascenoRafael/mqtt-simulator).

## License
Unless otherwise stated, the utilities in this repository are licensed
under version 2.0 of the Apache License and a copy of the license is
stored in the repository.
