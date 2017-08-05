"""Simple test example"""

import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from ..solution import answer
from lib.tools.timeout import *

class CountByCategoryTest(SparkTestingBaseTestCase):
    """Simple hell world example test."""

    def test_select_with_dup(self):
        input = [('apple', 'fruit'),
                ('apple', 'fruit'),
                ('banana', 'fruit'),
                ('mac', '3c'),
                ('ipad', '3c'),
                ('ipad', '3c'),
                ('ipad', '3c')]

        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, 'fruit')
        expected = self.sc.parallelize(['apple', 'banana'])
        self.assertTrue(self.assertRDDEquals(expected, result))

  
    def test_select_without_dup(self):
        input = [('apple', 'fruit'),
                ('banana', 'fruit'),
                ('mac', '3c'),
                ('ipad', '3c')]

        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, 'fruit')
        expected = self.sc.parallelize(['apple', 'banana'])
        self.assertTrue(self.assertRDDEquals(expected, result))

    def test_select_filter_number(self):
        input = [('apple', 1),
                ('banana', 5),
                ('mac', 2),
                ('ipad', 3)]

        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, 5)
        expected = self.sc.parallelize(['banana'])
        self.assertTrue(self.assertRDDEquals(expected, result))

if __name__ == "__main__":
    unittest2.main()
