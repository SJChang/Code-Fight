def answer(rdd, keyword):
    try:
        result = rdd.filter(lambda x: x[1][0] == keyword).map(lambda x: x[1][1]).reduce(lambda x, y: x + y)
    except:
        result = None
    return result
