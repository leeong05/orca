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
                }

    def add_operators(self):
        self.operators = {}

    def set_returns(self):
        self.returns = None
        self.ranked_returns = None


class TestCycleTechnical(unittest.TestCase):

    def setUp(self):
        self.env = MyEnvironment()
        self.dates = self.env.dates

    def test_HT_DCPERIOD(self):
        class MyHT_DCPERIOD(OperatorHT_DCPERIOD):

            def __init__(self, name, **kwargs):
                super(MyHT_DCPERIOD, self).__init__(100, name, **kwargs)

        self.env.add_operator('ht_dcperiod', {
            'operator': MyHT_DCPERIOD,
            })
        string = 'ht_dcperiod(open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        data = self.env.get_data_value('open')
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_DCPERIOD(data[i].values[:100])[-1] == val)
        self.assertTrue(all(res))

    def test_HT_DCPHASE(self):
        class MyHT_DCPHASE(OperatorHT_DCPHASE):

            def __init__(self, name, **kwargs):
                super(MyHT_DCPHASE, self).__init__(100, name, **kwargs)

        self.env.add_operator('ht_dcphase', {
            'operator': MyHT_DCPHASE,
            })
        string = 'ht_dcphase(open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        data = self.env.get_data_value('open')
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_DCPHASE(data[i].values[:100])[-1] == val)
        self.assertTrue(all(res))

    def test_HT_PHASOR(self):
        class MyHT_PHASOR(OperatorHT_PHASOR):

            def __init__(self, name, **kwargs):
                super(MyHT_PHASOR, self).__init__(100, name, **kwargs)

        self.env.add_operator('ht_phasor', {
            'operator': MyHT_PHASOR,
            })
        string = 'ht_phasor(2, open)'
        gene = self.env.parse_string(string)
        self.assertFalse(gene.validate())

        string = 'ht_phasor(0, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        data = self.env.get_data_value('open')
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_PHASOR(data[i].values[:100])[0][-1] == val)
        self.assertTrue(all(res))

        string = 'ht_phasor(1, open)'
        gene = self.env.parse_string(string)
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_PHASOR(data[i].values[:100])[1][-1] == val)
        self.assertTrue(all(res))

    def test_HT_SINE(self):
        class MyHT_SINE(OperatorHT_SINE):

            def __init__(self, name, **kwargs):
                super(MyHT_SINE, self).__init__(100, name, **kwargs)

        self.env.add_operator('ht_sine', {
            'operator': MyHT_SINE,
            })
        string = 'ht_sine(2, open)'
        gene = self.env.parse_string(string)
        self.assertFalse(gene.validate())

        string = 'ht_sine(0, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        data = self.env.get_data_value('open')
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_SINE(data[i].values[:100])[0][-1] == val)
        self.assertTrue(all(res))

        string = 'ht_sine(1, open)'
        gene = self.env.parse_string(string)
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_SINE(data[i].values[:100])[1][-1] == val)
        self.assertTrue(all(res))

    def test_HT_TRENDMODE(self):
        class MyHT_TRENDMODE(OperatorHT_TRENDMODE):

            def __init__(self, name, **kwargs):
                super(MyHT_TRENDMODE, self).__init__(100, name, **kwargs)

        self.env.add_operator('ht_trendmode', {
            'operator': MyHT_TRENDMODE,
            })
        string = 'ht_trendmode(open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        ser = gene.eval(self.env, self.dates[99], self.dates[99]).iloc[0]
        data = self.env.get_data_value('open')
        res = []
        for i, val in ser.iteritems():
            res.append(talib.HT_TRENDMODE(data[i].values[:100])[-1] == val)
        self.assertTrue(all(res))


if __name__ == '__main__':

    unittest.main()
