"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from orca.universe import factory
from orca.universe.base import DataFilter


class FactoryTestCase(unittest.TestCase):

    def test_create_quote_filter(self):
        price = factory.create_quote_filter('Price', 'close')
        self.assertEqual(price.__name__, 'Price')

    def test_create_cap_filter(self):
        cap = factory.create_cap_filter('Cap', 'a_shares')
        self.assertTrue(issubclass(cap, DataFilter))
