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


class TestVolumeTechnical(unittest.TestCase):

    def setUp(self):
        self.env = MyEnvironment()
        self.dates = self.env.dates

    def test_AD(self):
        class MyAD(OperatorAD):

            def __init__(self, name, **kwargs):
                super(MyAD, self).__init__(100, name, **kwargs)

        self.env.add_operator('ad', {
            'operator': MyAD,
            })
        string = 'ad(high, low, close, volume)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        v = self.env.get_data_value('volume').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.AD(h[:100, i], l[:100, i], c[:100, i], v[:100, i])[-1] == val)
        self.assertTrue(all(res))

    def test_ADOSC(self):
        class MyADOSC(OperatorADOSC):

            def __init__(self, name, **kwargs):
                super(MyADOSC, self).__init__(100, name, **kwargs)

        self.env.add_operator('adosc', {
            'operator': MyADOSC,
            })
        string = 'adosc(3, 10, high, low, close, volume)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        v = self.env.get_data_value('volume').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.ADOSC(h[:100, i], l[:100, i], c[:100, i], v[:100, i])[-1] == val)
        self.assertTrue(all(res))

    def test_OBV(self):
        class MyOBV(OperatorOBV):

            def __init__(self, name, **kwargs):
                super(MyOBV, self).__init__(100, name, **kwargs)

        self.env.add_operator('obv', {
            'operator': MyOBV,
            })
        string = 'obv(high, volume)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        h = self.env.get_data_value('high').values
        v = self.env.get_data_value('volume').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.OBV(h[:100, i], v[:100, i])[-1] == val)
        self.assertTrue(all(res))


if __name__ == '__main__':

    unittest.main()
