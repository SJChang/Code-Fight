# Inverted Index

Inverted Index is an index data structure used in IR, which could be implemented by map-reduce intuitivly.
Given a set of files, return each word with a collection of file names. 

* file1:
```
Apache Spark is a fast and general-purpose cluster computing system.
```
* file2:
```
Apache Spark has an advanced DAG execution engine that supports acyclic data flow and in-memory computing.
```

**Input**: `val inputRDD = sc.wholeTextFiles("data/file1.txt,data/file2.txt")` 

**Output**: `[("Spark", Set("file1.txt", "file2.txt")), ("in-memory", Set("file2.txt")), ("is", Set("file1.txt")), ("advanced", Set("file2.txt")... ]`

