def answer(rdd, keyword):
    result = rdd.filter(lambda x: x[1][0] == keyword)

    return result
