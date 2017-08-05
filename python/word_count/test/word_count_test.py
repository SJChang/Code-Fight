"""Simple test example"""

import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from ..solution import answer
from lib.tools.timeout import *

class WordCountTest(SparkTestingBaseTestCase):
    """Simple hell world example test."""

    def test_order_by_key(self):
        """Test a parallelize & collect."""
        input = ["hello world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('world', 1), ('hello', 1)])
        assert self.assertRDDEquals(expected, result) == True
    
    def test_order_by_value(self):
        input = ["hello world world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('world', 2), ('hello',1)])
        assert self.assertRDDEquals(expected, result) == True

    def test_same_value(self):
        input = ["hello hello world world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('hello', 2), ('world',2)])
        assert self.assertRDDEquals(expected, result) == True

if __name__ == "__main__":
    unittest2.main()
