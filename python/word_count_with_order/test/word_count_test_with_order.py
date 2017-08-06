"""Simple test example"""

import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from ..solution import answer
from lib.tools.timeout import *

class WordCountWithOrderTest(SparkTestingBaseTestCase):
    """Simple hell world example test."""

    def test_order_by_key(self):
        """Test a parallelize & collect."""
        input = ["hello world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('hello', 1), ('world',1)])
        self.assertTrue(self.assertRDDEqualsWithOrder(expected, result))
    
    def test_order_by_value(self):
        input = ["hello world world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('world', 2), ('hello',1)])
        self.assertTrue(self.assertRDDEqualsWithOrder(expected, result))

    def test_same_value(self):
        input = ["hello hello world world"]
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('hello', 2), ('world',2)])
        self.assertTrue(self.assertRDDEqualsWithOrder(expected, result))

if __name__ == "__main__":
    unittest2.main()
