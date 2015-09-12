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

    def test_LINEARREG(self):
        self.env.add_operator('linearreg', {
            'operator': OperatorLINEARREG,
            })
        string = 'linearreg(14, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[12], self.dates[-1])
        df = gene.eval(self.env, self.dates[13], self.dates[14])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i in df.columns:
            res0.append(talib.LINEARREG(o[:14, i], timeperiod=14)[-1] == ser0[i])
            res1.append(talib.LINEARREG(o[1:14+1, i], timeperiod=14)[-1] == ser1[i])
            res.append(talib.LINEARREG(o[:14+1, i], timeperiod=14)[-1] == ser1[i])
        self.assertTrue(all(res0) and all(res1) and all(res))

    def test_LINEARREG_ANGLE(self):
        self.env.add_operator('linearreg', {
            'operator': OperatorLINEARREG_ANGLE,
            })
        string = 'linearreg(14, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[12], self.dates[-1])
        df = gene.eval(self.env, self.dates[13], self.dates[14])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i in df.columns:
            res0.append(talib.LINEARREG_ANGLE(o[:14, i], timeperiod=14)[-1] == ser0[i])
            res1.append(talib.LINEARREG_ANGLE(o[1:14+1, i], timeperiod=14)[-1] == ser1[i])
            res.append(talib.LINEARREG_ANGLE(o[:14+1, i], timeperiod=14)[-1] == ser1[i])
        self.assertTrue(all(res0) and all(res1) and all(res))

    def test_LINEARREG_INTERCEPT(self):
        self.env.add_operator('linearreg', {
            'operator': OperatorLINEARREG_INTERCEPT,
            })
        string = 'linearreg(14, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[12], self.dates[-1])
        df = gene.eval(self.env, self.dates[13], self.dates[14])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i in df.columns:
            res0.append(talib.LINEARREG_INTERCEPT(o[:14, i], timeperiod=14)[-1] == ser0[i])
            res1.append(talib.LINEARREG_INTERCEPT(o[1:14+1, i], timeperiod=14)[-1] == ser1[i])
            res.append(talib.LINEARREG_INTERCEPT(o[:14+1, i], timeperiod=14)[-1] == ser1[i])
        self.assertTrue(all(res0) and all(res1) and all(res))

    def test_LINEARREG_SLOPE(self):
        self.env.add_operator('linearreg', {
            'operator': OperatorLINEARREG_SLOPE,
            })
        string = 'linearreg(14, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[12], self.dates[-1])
        df = gene.eval(self.env, self.dates[13], self.dates[14])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i in df.columns:
            res0.append(talib.LINEARREG_SLOPE(o[:14, i], timeperiod=14)[-1] == ser0[i])
            res1.append(talib.LINEARREG_SLOPE(o[1:14+1, i], timeperiod=14)[-1] == ser1[i])
            res.append(talib.LINEARREG_SLOPE(o[:14+1, i], timeperiod=14)[-1] == ser1[i])
        self.assertTrue(all(res0) and all(res1) and all(res))

    def test_TSF(self):
        self.env.add_operator('linearreg', {
            'operator': OperatorTSF,
            })
        string = 'linearreg(14, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[12], self.dates[-1])
        df = gene.eval(self.env, self.dates[13], self.dates[14])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i in df.columns:
            res0.append(talib.TSF(o[:14, i], timeperiod=14)[-1] == ser0[i])
            res1.append(talib.TSF(o[1:14+1, i], timeperiod=14)[-1] == ser1[i])
            res.append(talib.TSF(o[:14+1, i], timeperiod=14)[-1] == ser1[i])
        self.assertTrue(all(res0) and all(res1) and all(res))


if __name__ == '__main__':

    unittest.main()
