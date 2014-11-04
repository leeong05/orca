"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import logging
import unittest

import numpy as np
import pandas as pd

from orca.alpha import (
        AlphaBase,
        BacktestingAlpha,
        ProductionAlpha)


class AlphaBaseDummy(AlphaBase):

    def generate(self, date):
        raise NotImplementedError


class AlphaBaseTestCase(unittest.TestCase):

    def setUp(self):
        self.alpha = AlphaBaseDummy()

    def tearDown(self):
        self.alpha = None

    def test_is_abstract_class(self):
        self.assertRaises(TypeError, AlphaBase)

    def test_logger_name(self):
        self.assertEqual(self.alpha.logger.name, AlphaBase.LOGGER_NAME)

    def test_debug_mode_default_on(self):
        self.assertEqual(self.alpha.logger.level, logging.DEBUG)

    def test_debug_mode_reset(self):
        self.alpha.set_debug_mode(False)
        self.assertEqual(self.alpha.logger.level, logging.INFO)
        # change status back
        self.alpha.set_debug_mode(True)

    def test_generate(self):
        self.assertRaises(NotImplementedError, self.alpha.generate, '')


SEED = 1024
chars = [chr(i) for i in range(97, 97+26)]

class BacktestingAlphaDummy(BacktestingAlpha):

    def generate(self, date):
        self.alphas[date] = pd.Series(np.random.randn(len(chars)), index=chars)

class BacktestingAlphaTestCase(unittest.TestCase):

    def setUp(self):
        self.alpha = BacktestingAlphaDummy()
        self.date = '20140101'
        self.dates_str = ['20140101', '20140201']
        self.dates_dt = pd.to_datetime(self.dates_str)

    def tearDown(self):
        self.alpha = None

    def test_generate_add_key(self):
        self.alpha.generate(self.date)
        self.assertIn(self.date, self.alpha.alphas)
        del self.alpha.alphas[self.date]

    def test_generate_once_result(self):
        np.random.seed(SEED)
        self.alpha.generate(self.date)
        np.random.seed(SEED)
        series = pd.Series(np.random.randn(len(chars)), index=chars)
        self.assertTrue((series == self.alpha.alphas[self.date]).all())
        del self.alpha.alphas[self.date]

    def test_get_alphas_return_dataframe(self):
        self.assertIsInstance(self.alpha.get_alphas(), pd.DataFrame)

    def test_get_alphas_datetime_index(self):
        np.random.seed(SEED)
        for date in self.dates_str:
            self.alpha.generate(date)

        alphas = self.alpha.get_alphas()
        self.assertTrue((alphas.index == self.dates_dt).all())

    def test_get_alphas_datetime_index_false(self):
        np.random.seed(SEED)
        for date in self.dates_str:
            self.alpha.generate(date)

        alphas = self.alpha.get_alphas(datetime_index=False)
        self.assertListEqual(list(alphas.index), self.dates_str)

    def test_get_alphas_columns(self):
        np.random.seed(SEED)
        for date in self.dates_str:
            self.alpha.generate(date)

        alphas = self.alpha.get_alphas(datetime_index=False)
        self.assertListEqual(list(alphas.columns), chars)


return_str = "Inside the call 'generate'"

class ProductionAlphaDummy(ProductionAlpha):

    def generate(self, date):
        return return_str

class ProductionAlphaTestCase(unittest.TestCase):

    def setUp(self):
        self.alpha = ProductionAlphaDummy()

    def tearDown(self):
        self.alpha = None

    def test_generate(self):
        self.assertEqual(self.alpha.generate(''), return_str)
