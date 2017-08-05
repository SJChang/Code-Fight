"""Simple test example"""

import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from ..solution import answer
from lib.tools.timeout import *

class CountByCategoryTest(SparkTestingBaseTestCase):
    """Simple hell world example test."""

    def test_count_by_category(self):
        """Test a parallelize & collect."""
        input = [('apple', 'fruit'),
                ('apple', 'fruit'),
                ('banana', 'fruit'),
                ('mac', '3c'),
                ('ipad', '3c'),
                ('ipad', '3c'),
                ('ipad', '3c')]

        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('3c','ipad', 3), ('3c', 'mac', 1), ('fruit', 'apple', 2), ('fruit', 'banana', 1)])
        self.assertTrue(self.assertRDDEqualsWithOrder(expected, result))

    def test_count_by_category_with_order(self):
        """Test a parallelize & collect."""
        input = [('mac', '3c'),
                ('apple', 'fruit'),
                ('apple', 'fruit'),
                ('mac','3c'),
                ('banana', 'fruit'),
                ('mac', '3c'),
                ('ipad', '3c'),
                ('ipad', '3c')]

        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd)
        expected = self.sc.parallelize([('3c', 'mac', 3), ('3c', 'ipad', 2), ('fruit', 'apple', 2), ('fruit', 'banana', 1)])
        self.assertTrue(self.assertRDDEqualsWithOrder(expected, result))

   
if __name__ == "__main__":
    unittest2.main()
