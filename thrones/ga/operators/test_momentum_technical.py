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


class TestMomentumTechnical(unittest.TestCase):

    def setUp(self):
        self.env = MyEnvironment()
        self.dates = self.env.dates

    def test_ADX(self):
        class MyADX(OperatorADX):

            def __init__(self, name, **kwargs):
                super(MyADX, self).__init__(100, name, **kwargs)

        self.env.add_operator('adx', {
            'operator': MyADX,
            })
        string = 'adx(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.ADX(h[:100, i], l[:100, i], c[:100, i], 14)[-1] == val)
        for i, val in ser1.iteritems():
            res1.append(talib.ADX(h[1:100+1, i], l[1:100+1, i], c[1:100+1, i], 14)[-1] == val)
            res.append(talib.ADX(h[:100+1, i], l[:100+1, i], c[:100+1, i], 14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_ADXR(self):
        class MyADXR(OperatorADXR):

            def __init__(self, name, **kwargs):
                super(MyADXR, self).__init__(100, name, **kwargs)

        self.env.add_operator('adxr', {
            'operator': MyADXR,
            })
        string = 'adxr(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.ADXR(h[:100, i], l[:100, i], c[:100, i], 14)[-1] == val)
        for i, val in ser1.iteritems():
            res1.append(talib.ADXR(h[1:100+1, i], l[1:100+1, i], c[1:100+1, i], 14)[-1] == val)
            res.append(talib.ADXR(h[:100+1, i], l[:100+1, i], c[:100+1, i], 14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_APO(self):
        self.env.add_operator('apo', {
            'operator': OperatorAPO,
            })
        string = 'apo(12, 26, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[24], self.dates[-1])
        df = gene.eval(self.env, self.dates[25], self.dates[26])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.APO(o[:26+1, i], fastperiod=12, slowperiod=26)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_AROON(self):
        self.env.add_operator('aroon', {
            'operator': OperatorAROON,
            })
        string = 'aroon(0, 14, high, low)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[13], self.dates[-1])
        df = gene.eval(self.env, self.dates[14], self.dates[15])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.AROON(h[:15+1, i], l[:15+1, i], timeperiod=14)[0]
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_AROONOSC(self):
        self.env.add_operator('aroonosc', {
            'operator': OperatorAROONOSC,
            })
        string = 'aroonosc(14, high, low)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[13], self.dates[-1])
        df = gene.eval(self.env, self.dates[14], self.dates[15])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.AROONOSC(h[:15+1, i], l[:15+1, i], timeperiod=14)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_BOP(self):
        self.env.add_operator('bop', {
            'operator': OperatorBOP,
            })
        string = 'bop(open, high, low, close)'
        gene = self.env.parse_string(string)
        df = gene.eval(self.env, self.dates[14], self.dates[15])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.BOP(o[:15+1, i], h[:15+1, i], l[:15+1, i], c[:15+1, i])
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_CCI(self):
        self.env.add_operator('cci', {
            'operator': OperatorCCI,
            })
        string = 'cci(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[12], self.dates[-1])
        df = gene.eval(self.env, self.dates[13], self.dates[14])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.CCI(h[:14+1, i], l[:14+1, i], c[:14+1, i], 14)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_CMO(self):
        class MyCMO(OperatorCMO):

            def __init__(self, name, **kwargs):
                super(MyCMO, self).__init__(100, name, **kwargs)

        self.env.add_operator('cmo', {
            'operator': MyCMO,
            })
        string = 'cmo(14, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.CMO(o[:100, i], timeperiod=14)[-1] == val)
        for i, val in ser1.iteritems():
            res1.append(talib.CMO(o[1:100+1, i], timeperiod=14)[-1] == val)
            res.append(talib.CMO(o[:100+1, i], timeperiod=14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_DX(self):
        class MyDX(OperatorDX):

            def __init__(self, name, **kwargs):
                super(MyDX, self).__init__(100, name, **kwargs)

        self.env.add_operator('dx', {
            'operator': MyDX,
            })
        string = 'dx(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.DX(h[:100, i], l[:100, i], c[:100, i], timeperiod=14)[-1] == val)
        for i, val in ser1.iteritems():
            res1.append(talib.DX(h[1:100+1, i], l[1:100+1, i], c[1:100+1, i], timeperiod=14)[-1] == val)
            res.append(talib.DX(h[:100+1, i], l[:100+1, i], c[:100+1, i], timeperiod=14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_MACD(self):
        class MyMACD(OperatorMACD):

            def __init__(self, name, **kwargs):
                super(MyMACD, self).__init__(100, name, **kwargs)

        self.env.add_operator('macd', {
            'operator': MyMACD,
            })
        string = 'macd(0, 12, 26, 9, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.MACD(o[:100, i],
                fastperiod=12, slowperiod=26, signalperiod=9)[0][-1] == val)
        for i, val in ser1.iteritems():
            res1.append(talib.MACD(o[1:100+1, i],
                fastperiod=12, slowperiod=26, signalperiod=9)[0][-1] == val)
            res.append(talib.MACD(o[:100+1, i],
                fastperiod=12, slowperiod=26, signalperiod=9)[0][-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_MACDEXT(self):
        self.env.add_operator('macdext', {
            'operator': OperatorMACDEXT,
            })
        string = 'macdext(0, 12, 26, 9, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[32], self.dates[-1])
        df = gene.eval(self.env, self.dates[33], self.dates[34])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.MACDEXT(o[:34+1, i],
                    fastperiod=12, slowperiod=26, signalperiod=9)[0]
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_MFI(self):
        self.env.add_operator('mfi', {
            'operator': OperatorMFI,
            })
        string = 'mfi(14, high, low, close, volume)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[13], self.dates[-1])
        df = gene.eval(self.env, self.dates[14], self.dates[15])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        v = self.env.get_data_value('volume').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.MFI(h[:15+1, i], l[:15+1, i], c[:15+1, i], v[:15+1, i], 14)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_MINUS_DI(self):
        class MyMINUS_DI(OperatorMINUS_DI):

            def __init__(self, name, **kwargs):
                super(MyMINUS_DI, self).__init__(100, name, **kwargs)

        self.env.add_operator('minus_di', {
            'operator': MyMINUS_DI,
            })
        string = 'minus_di(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.MINUS_DI(h[:100, i], l[:100, i], c[:100, i],
                timeperiod=14)[-1] == val)
        for i, val in ser1.iteritems():
            res0.append(talib.MINUS_DI(h[1:100+1, i], l[1:100+1, i], c[1:100+1, i],
                timeperiod=14)[-1] == val)
            res.append(talib.MINUS_DI(h[:100+1, i], l[:100+1, i], c[:100+1, i],
                timeperiod=14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_MINUS_DM(self):
        class MyMINUS_DM(OperatorMINUS_DM):

            def __init__(self, name, **kwargs):
                super(MyMINUS_DM, self).__init__(100, name, **kwargs)

        self.env.add_operator('minus_dm', {
            'operator': MyMINUS_DM,
            })
        string = 'minus_dm(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.MINUS_DM(h[:100, i], l[:100, i],
                timeperiod=14)[-1] == val)
        for i, val in ser1.iteritems():
            res0.append(talib.MINUS_DM(h[1:100+1, i], l[1:100+1, i],
                timeperiod=14)[-1] == val)
            res.append(talib.MINUS_DM(h[:100+1, i], l[:100+1, i],
                timeperiod=14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_MOM(self):
        self.env.add_operator('mom', {
            'operator': OperatorMOM,
            })
        string = 'mom(10, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[9], self.dates[-1])
        df = gene.eval(self.env, self.dates[10], self.dates[11])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.MOM(o[:11+1, i],
                    timeperiod=10)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_PLUS_DI(self):
        class MyPLUS_DI(OperatorPLUS_DI):

            def __init__(self, name, **kwargs):
                super(MyPLUS_DI, self).__init__(100, name, **kwargs)

        self.env.add_operator('minus_di', {
            'operator': MyPLUS_DI,
            })
        string = 'minus_di(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.PLUS_DI(h[:100, i], l[:100, i], c[:100, i],
                timeperiod=14)[-1] == val)
        for i, val in ser1.iteritems():
            res0.append(talib.PLUS_DI(h[1:100+1, i], l[1:100+1, i], c[1:100+1, i],
                timeperiod=14)[-1] == val)
            res.append(talib.PLUS_DI(h[:100+1, i], l[:100+1, i], c[:100+1, i],
                timeperiod=14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_PLUS_DM(self):
        class MyPLUS_DM(OperatorPLUS_DM):

            def __init__(self, name, **kwargs):
                super(MyPLUS_DM, self).__init__(100, name, **kwargs)

        self.env.add_operator('minus_dm', {
            'operator': MyPLUS_DM,
            })
        string = 'minus_dm(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.PLUS_DM(h[:100, i], l[:100, i],
                timeperiod=14)[-1] == val)
        for i, val in ser1.iteritems():
            res0.append(talib.PLUS_DM(h[1:100+1, i], l[1:100+1, i],
                timeperiod=14)[-1] == val)
            res.append(talib.PLUS_DM(h[:100+1, i], l[:100+1, i],
                timeperiod=14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_PPO(self):
        self.env.add_operator('ppo', {
            'operator': OperatorPPO,
            })
        string = 'ppo(12, 26, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[24], self.dates[-1])
        df = gene.eval(self.env, self.dates[25], self.dates[26])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.PPO(o[:26+1, i],
                    fastperiod=12, slowperiod=26)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_RSI(self):
        class MyRSI(OperatorRSI):

            def __init__(self, name, **kwargs):
                super(MyRSI, self).__init__(100, name, **kwargs)

        self.env.add_operator('rsi', {
            'operator': MyRSI,
            })
        string = 'rsi(14, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.RSI(o[:100, i], timeperiod=14)[-1] == val)
        for i, val in ser1.iteritems():
            res1.append(talib.RSI(o[1:100+1, i], timeperiod=14)[-1] == val)
            res.append(talib.RSI(o[:100+1, i], timeperiod=14)[-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_STOCH(self):
        self.env.add_operator('stoch', {
            'operator': OperatorSTOCH,
            })
        string = 'stoch(0, 5, 3, 3, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[7], self.dates[-1])
        df = gene.eval(self.env, self.dates[8], self.dates[9])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.STOCH(h[:9+1, i], l[:9+1, i], c[:9+1, i],
                    fastk_period=5, slowk_period=3, slowd_period=3)[0]
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_STOCHF(self):
        self.env.add_operator('stochf', {
            'operator': OperatorSTOCHF,
            })
        string = 'stochf(0, 5, 3, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[5], self.dates[-1])
        df = gene.eval(self.env, self.dates[6], self.dates[7])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.STOCHF(h[:7+1, i], l[:7+1, i], c[:7+1, i],
                    fastk_period=5, fastd_period=3)[0]
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_STOCHSTOCHRSI(self):
        class MySTOCHRSI(OperatorSTOCHRSI):

            def __init__(self, name, **kwargs):
                super(MySTOCHRSI, self).__init__(100, name, **kwargs)

        self.env.add_operator('stochrsi', {
            'operator': MySTOCHRSI,
            })
        string = 'stochrsi(0, 14, 5, 3, open)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[98], self.dates[-1])
        df = gene.eval(self.env, self.dates[99], self.dates[100])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        o = self.env.get_data_value('open').values
        res0, res1, res = [], [], []
        for i, val in ser0.iteritems():
            res0.append(talib.STOCHRSI(o[:100, i],
                timeperiod=14, fastk_period=5, fastd_period=3)[0][-1] == val)
        for i, val in ser1.iteritems():
            res1.append(talib.STOCHRSI(o[1:100+1, i],
                timeperiod=14, fastk_period=5, fastd_period=3)[0][-1] == val)
            res.append(talib.STOCHRSI(o[:100+1, i],
                timeperiod=14, fastk_period=5, fastd_period=3)[0][-1] != val)
        self.assertTrue(all(res0) and all(res1) and any(res))

    def test_ULTOSC(self):
        self.env.add_operator('ultosc', {
            'operator': OperatorULTOSC,
            })
        string = 'ultosc(7, 14, 28, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[27], self.dates[-1])
        df = gene.eval(self.env, self.dates[28], self.dates[29])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.ULTOSC(h[:29+1, i], l[:29+1, i], c[:29+1, i],
                    timeperiod1=7, timeperiod2=14, timeperiod3=28)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))

    def test_WILLR(self):
        self.env.add_operator('willr', {
            'operator': OperatorWILLR,
            })
        string = 'willr(14, high, low, close)'
        gene = self.env.parse_string(string)
        self.assertRaises(IndexError, gene.eval, self.env, self.dates[12], self.dates[-1])
        df = gene.eval(self.env, self.dates[13], self.dates[14])
        ser0, ser1 = df.iloc[0], df.iloc[1]
        h = self.env.get_data_value('high').values
        l = self.env.get_data_value('low').values
        c = self.env.get_data_value('close').values
        res0, res1 = [], []
        for i in ser0.index:
            res = talib.WILLR(h[:14+1, i], l[:14+1, i], c[:14+1, i], 14)
            val0, val1 = res[-2], res[-1]
            res0.append(ser0[i] == val0)
            res1.append(ser1[i] == val1)
        self.assertTrue(all(res0) and all(res1))


if __name__ == '__main__':

    unittest.main()
