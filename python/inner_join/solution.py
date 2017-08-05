def answer(rddA, rddB):
    result = rddA.map(lambda x: (x[1], x[0])).join(rddB)
    return result
