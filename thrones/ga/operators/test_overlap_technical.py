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


class TestOverlapTechnical(unittest.TestCase):

    def setUp(self):
        self.env = MyEnvironment()
        self.dates = self.env.dates

    def test_BBANDS(self):
        self.env.add_operator('bbands', {
            'operator': OperatorBBANDS,
            })
        string = 'bbands(1, 100, 10, 10, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.BBANDS(o[:100, i], 100, 10, 10)[1][-1] == val)
        self.assertTrue(all(res))

    def test_DEMA(self):
        class MyDEMA(OperatorDEMA):

            def __init__(self, name, **kwargs):
                super(MyDEMA, self).__init__(100, name, **kwargs)

        self.env.add_operator('dema', {
            'operator': MyDEMA,
            })
        string = 'dema(30, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.DEMA(o[:100, i], 30)[-1] == val)
        self.assertTrue(all(res))

    def test_EMA(self):
        class MyEMA(OperatorEMA):

            def __init__(self, name, **kwargs):
                super(MyEMA, self).__init__(100, name, **kwargs)

        self.env.add_operator('ema', {
            'operator': MyEMA,
            })
        string = 'ema(30, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.EMA(o[:100, i], 30)[-1] == val)
        self.assertTrue(all(res))

    def test_HT_TRENDLINE(self):
        class MyHT_TRENDLINE(OperatorHT_TRENDLINE):

            def __init__(self, name, **kwargs):
                super(MyHT_TRENDLINE, self).__init__(100, name, **kwargs)

        self.env.add_operator('ht_trendline', {
            'operator': MyHT_TRENDLINE,
            })
        string = 'ht_trendline(open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_TRENDLINE(o[:100, i])[-1] == val)
        self.assertTrue(all(res))

    def test_KAMA(self):
        class MyKAMA(OperatorKAMA):

            def __init__(self, name, **kwargs):
                super(MyKAMA, self).__init__(100, name, **kwargs)

        self.env.add_operator('kama', {
            'operator': MyKAMA,
            })
        string = 'kama(30, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.KAMA(o[:100, i], 30)[-1] == val)
        self.assertTrue(all(res))

    def test_MAMA(self):
        class MyMAMA(OperatorMAMA):

            def __init__(self, name, **kwargs):
                super(MyMAMA, self).__init__(100, name, **kwargs)

        self.env.add_operator('mama', {
            'operator': MyMAMA,
            })
        string = 'mama(1, 0.5, 0.05, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.MAMA(o[:100, i], 0.5, 0.05)[1][-1] == val)
        self.assertTrue(all(res))

    def test_SAR(self):
        class MySAR(OperatorSAR):

            def __init__(self, name, **kwargs):
                super(MySAR, self).__init__(100, name, **kwargs)

        self.env.add_operator('sar', {
            'operator': MySAR,
            })
        string = 'sar(0.02, 0.2, high, low)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.SAR(h[:100, i], l[:100, i], 0.02, 0.2)[-1] == val)
        self.assertTrue(all(res))

    def test_T3(self):
        class MyT3(OperatorT3):

            def __init__(self, name, **kwargs):
                super(MyT3, self).__init__(100, name, **kwargs)

        self.env.add_operator('t3', {
            'operator': MyT3,
            })
        string = 't3(5, 0.7, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.T3(o[:100, i], 5, 0.7)[-1] == val)
        self.assertTrue(all(res))

    def test_TEMA(self):
        class MyTEMA(OperatorTEMA):

            def __init__(self, name, **kwargs):
                super(MyTEMA, self).__init__(100, name, **kwargs)

        self.env.add_operator('tema', {
            'operator': MyTEMA,
            })
        string = 'tema(30, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.TEMA(o[:100, i], 30)[-1] == val)
        self.assertTrue(all(res))

    def test_TRIMA(self):
        self.env.add_operator('trima', {
            'operator': OperatorTRIMA,
            })
        string = 'trima(30, open)'
        gene = self.env.parse_string(string)
        ser = gene.eval(self.env, self.dates[29], self.dates[29]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.TRIMA(o[:30, i], 30)[-1] == val)
        self.assertTrue(all(res))

    def test_WMA(self):
        self.env.add_operator('trima', {
            'operator': OperatorWMA,
            })
        string = 'trima(30, open)'
        gene = self.env.parse_string(string)
        ser = gene.eval(self.env, self.dates[29], self.dates[29]).iloc[0]
        o = self.env.get_data_value('open').values
        res = []
        for i, val in ser.iteritems():
            res.append(talib.WMA(o[:30, i], 30)[-1] == val)
        self.assertTrue(all(res))


if __name__ == '__main__':

    unittest.main()
