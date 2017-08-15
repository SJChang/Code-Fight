# Inner Join

Given two datasets, returns all records from the left table (rddA), and the matched records from the right table (rddB). The result is NULL from the right side, if there is no match.

**Input**: 

rddA = `sc.parallelize([('fruit','apple'), ('fruit','apple'), ('fruit','banana'), ('3c','mac')])`

rddB = `sc.parallelize([('apple', 5), ('banana', 3), ('kiwi', 10)])`   
 
**Output**: 

`[('apple', ('fruit', 5)), ('apple', ('fruit', 5)), ('banana', ('fruit', 3)), ('mac', ('3c', None))]` (RDD)


