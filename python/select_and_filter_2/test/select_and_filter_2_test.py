"""Simple test example"""

import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from ..solution import answer
from lib.tools.timeout import *

class SelectFilter2Test(SparkTestingBaseTestCase):
    """Simple hell world example test."""

    def test_select_filter_by_count(self):
        input = ["hello hello world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, 2)
        expected = self.sc.parallelize(["hello"])
        self.assertRDDEqualsWithOrder(expected, result)

    def test_select_filter_by_count_distinct(self):
        input = ["hello hello world world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, 2)
        expected = self.sc.parallelize(["hello", "world"])
        self.assertRDDEqualsWithOrder(expected, result)

    def test_return_none(self):
        input = ["hello hello world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, 3)
        expected = self.sc.parallelize([])
        self.assertRDDEqualsWithOrder(expected, result)

    
if __name__ == "__main__":
    unittest2.main()
