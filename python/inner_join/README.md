# Inner Join

**Input**: 

rddA = `sc.parallelize([('fruit','apple'), ('fruit','apple'), ('fruit','banana'), ('3c','mac')])`

rddB = `sc.parallelize([('apple', 5), ('banana', 3), ('kiwi', 10)])`   
 
**Output**: 

`[('apple', ('fruit', 5)), ('apple', ('fruit', 5)), ('banana', ('fruit', 3))]` (RDD)


