
def answer(data):
    result = data.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)\
                 .sortBy(lambda x : x[0]).sortBy(lambda x: x[1],  ascending=False)
    return result
