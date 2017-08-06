"""Simple test example"""

import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from ..solution import answer
from lib.tools.timeout import *

class InnerJoinTest(SparkTestingBaseTestCase):
    """Simple hell world example test."""

    def test_basic_join(self):
        inputA = [('fruit', 'apple'),
                ('fruit','apple'),
                ('fruit','banana'),
                ('3c', 'mac')]

        inputB = [('apple', 5), 
                ('banana', 3), 
                ('kiwi', 10)]

        rddA = self.sc.parallelize(inputA)
        rddB = self.sc.parallelize(inputB)
        timeout_ = timeout(answer, 5)
        result = timeout_(rddA, rddB)
        expected = self.sc.parallelize([('apple', ('fruit', 5)), 
                                        ('apple', ('fruit', 5)), 
                                        ('banana', ('fruit', 3))])
        self.assertTrue(self.assertRDDEquals(expected, result))

if __name__ == "__main__":
    unittest2.main()
