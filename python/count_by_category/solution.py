
def answer(rdd):
    result = rdd.map(lambda x: ((x[1], x[0]),1) )\
                .reduceByKey(lambda x, y: x + y)\
                .sortBy(lambda x: (x[0][0], x[1]), ascending=False)\
                .sortBy(lambda x: x[0][0])\
                .map(lambda x: (x[0][0], x[0][1], x[1]))
    return result
