
# Kraken

A high performance readonly in memory key/value store.

Provides distributed/load balanced API using ZMQ, with sample clients in Python/PHP/C++.

Supports multiple namespaces and simple query model with additional random access and cursor. 

The store itself is just an interface supporting many implementations.  The highest performance is an offset table using binary serach and 2G blocks.  Other more basic data structures are also provided as sample implementations.  

Benchmarked against and outperforms Kyoto Tykoon/Cabinet and Google LevelDB.  

