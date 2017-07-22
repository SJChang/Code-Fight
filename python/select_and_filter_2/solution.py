def answer(rdd, number):
    result = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x ,y: x + y).filter(lambda x: x[1] == number).map(lambda x:x[0]).distinct()

    return result
