import unittest

import numpy as np
import pandas as pd
import talib

from ...util.datetime import to_dateint
from ..environment import Environment

from .technical import *


class MyEnvironment(Environment):

    def set_dates(self):
        self.dates = to_dateint(pd.date_range('20140101', '20150201', freq='B'))
        self.size = (len(self.dates), 100)

    def add_datas(self):
        self.datas = {
                'open': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    },
                'high': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    },
                'low': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    },
                'close': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    },
                'volume': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    },
                }

    def add_operators(self):
        self.operators = {}

    def set_returns(self):
        self.returns = None
        self.ranked_returns = None


class TestVolatilityTechnical(unittest.TestCase):

    def setUp(self):
        self.env = MyEnvironment()
        self.dates = self.env.dates

    def test_ATR(self):
        class MyATR(OperatorATR):

            def __init__(self, name, **kwargs):
                super(MyATR, self).__init__(100, name, **kwargs)

        self.env.add_operator('atr', {
            'operator': MyATR,
            })
        string = 'atr(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.ATR(h[:100, i], l[:100, i], c[:100, i], 14)[-1] == val)
        self.assertTrue(all(res))

    def test_NATR(self):
        class MyNATR(OperatorNATR):

            def __init__(self, name, **kwargs):
                super(MyNATR, self).__init__(100, name, **kwargs)

        self.env.add_operator('natr', {
            'operator': MyNATR,
            })
        string = 'natr(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.NATR(h[:100, i], l[:100, i], c[:100, i], 14)[-1] == val)
        self.assertTrue(all(res))

    def test_TRANGE(self):
        self.env.add_operator('trange', {
            'operator': OperatorTRANGE,
            })
        string = 'trange(high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[0], self.dates[-1])
        ser = gene.eval(self.env, self.dates[1], self.dates[1]).iloc[0]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.TRANGE(h[:2, i], l[:2, i], c[:2, i])[-1] == val)
        self.assertTrue(all(res))


if __name__ == '__main__':

    unittest.main()
