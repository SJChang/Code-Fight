# N-Grams

N-Grams is a contiguous sequence of n items from a given sequence of text or speech. It is widely used in text mining and natural language processing tasks.

Given a input string, N of N-Grams and top frequency topN, return top n N-Grams and its counts.
You have to do some simple preprocessing:
    1. Transform all characters to lower case.
    2. Keep the hyphen between words, e.g., hard-working.


**Input**: 

```scala
val inputStr: String = """Spark SQL is a Spark module for structured data processing.
                          |Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide 
                          |Spark with more information about the structure of both the data and the 
                          |computation being performed. Internally, Spark SQL uses this extra information
                          |to perform extra optimizations. There are several ways to interact with Spark SQL
                          |including SQL and the Dataset API. One use of Spark SQL is to execute SQL queries.
                          |Spark SQL can also be used to read data from an existing Hive installation.
                          |When running SQL from within another programming language the results will 
                          |be returned as a Dataset/DataFrame. You can also interact with the SQL interface
                          |using the command-line or over JDBC/ODBC.""".stripMargin
val inputRDD = sc.parallelize(Seq(inputStr))
answer(inputRDD, 2, 5)
``` 

**Output**: 

```scala
Array(("spark sql", 6), ("interact with", 2), ("and the", 2), ("sql is", 2), ("can also", 2))
```
