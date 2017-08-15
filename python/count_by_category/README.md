# Count by Group

Given a list of datasets, please merge the same set into one group, and calculate it's value.

**Input**: rdd = `sc.parallelize([('apple', 'fruit'), ('apple', 'fruit'), ('banana', 'fruit'), ('mac', '3c'), ('ipad', '3c'), ('ipad', '3c'), ('ipad', '3c')])` 

**Output**: `[('3c','ipad', 3), ('3c', 'mac', 1), ('fruit', 'apple', 2), ('fruit', 'banana', 1)]` (RDD)


