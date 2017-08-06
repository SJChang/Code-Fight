"""Simple test example"""

import unittest2
from sparktestingbase.testcase import SparkTestingBaseTestCase
from ..solution import answer
from lib.tools.timeout import *

class SelectFilter3Test(SparkTestingBaseTestCase):
    """Simple hell world example test."""

    def test_select_filter_by_string(self):
        input = [(u'Some1', (u'ABC', 9989)),
                 (u'Some2', (u'XYZ', 235)),
                 (u'Some3', (u'BBB', 5379)),
                 (u'Some4', (u'ABC', 5379))]
        keyword = 'ABC'
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, keyword)
        expected = self.sc.parallelize(
                    [(u'Some1', (u'ABC', 9989)),
                     (u'Some4', (u'ABC', 5379))])
        self.assertRDDEqualsWithOrder(expected, result)

    def test_select_filter_by_string_2(self):
        input = [(u'Some1', (u'ABC', 9989)),
                 (u'Some2', (u'XYZ', 235)),
                 (u'Some3', (u'BBB', 5379)),
                 (u'Some4', (u'ABC', 5379))]
        keyword = 'QQ'
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, keyword)
        expected = self.sc.parallelize([])
        self.assertRDDEqualsWithOrder(expected, result)

    def test_select_filter_by_string_3(self):
        input = [(u'Some1', (u'ABC', 9989)),
                 (u'Some2', (u'XYZ', 235)),
                 (u'Some3', (u'BBB', 5379)),
                 (u'Some4', (u'ABC', 5379))]
        keyword = 'XYZ'
        rdd = self.sc.parallelize(input)
        timeout_ = timeout(answer, 5)
        result = timeout_(rdd, keyword)
        expected = self.sc.parallelize([(u'Some2', (u'XYZ', 235))])
        self.assertRDDEqualsWithOrder(expected, result)

if __name__ == "__main__":
    unittest2.main()
