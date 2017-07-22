
def answer(rdd, condition):
    result = rdd.filter(lambda x: x[1] == condition).map(lambda x: x[0]).distinct()
    return result
