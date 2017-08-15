# select and filter

Given a dataset and a condition word, if the second element in tuple equals to the condition, output the first element. 

**Input**: rdd = `sc.parallelize([('apple', 'fruit'), ('apple', 'fruit'), ('banana', 'fruit'), ('mac', '3c')])`
           condition = `fruit`
 
**Output**: `['apple', 'banana']` (RDD)


