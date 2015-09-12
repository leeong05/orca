import unittest

import numpy as np
import pandas as pd

from ..util.datetime import to_dateint
from ..util.testing import (
        almost_zero,
        almost_equal,
        frame_equal,
        )
from ..operation import api

from .environment import Environment
from .operators import *


class MyEnvironment(Environment):

    def set_dates(self):
        self.dates = to_dateint(pd.date_range('20150101', '20150201', freq='B'))
        self.size = (len(self.dates), 100)

    def add_datas(self):
        self.datas = {
                'open1': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    'dimension': 'CNY',
                    },
                'open2': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    'dimension': 'USD',
                    },
                'close1': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    'dimension': 'CNY',
                    },
                'close2': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    'dimension': 'USD',
                    },
                'faceless': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    'dimension': '?',
                    },
                'blackhole': {
                    'value': pd.DataFrame(np.random.randn(*self.size), index=self.dates),
                    'dimension': '*',
                    },
                'group': {
                    'value': pd.DataFrame(np.outer([1]*self.size[0],
                        np.random.randint(5, size=self.size[1])), index=self.dates),
                    },
                }

    def add_operators(self):
        self.operators = {}

    def set_returns(self):
        self.returns = None
        self.ranked_returns = None


class TestOperators(unittest.TestCase):

    def setUp(self):
        self.env = MyEnvironment()
        self.dates = self.env.dates
        self.date1, self.date2 = self.dates[0], self.dates[-1]

    def test_add(self):
        self.env.add_operator('add', {
            'operator': OperatorAdd,
            })
        string1 = 'add(open1, open2)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'add(open1, close1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY')
        string3 = 'add(open1, faceless)'
        gene3 = self.env.parse_string(string3)
        self.assertTrue(gene3.validate())
        self.assertEqual(gene3.dimension, 'CNY')
        string4 = 'add(open1, blackhole)'
        gene4 = self.env.parse_string(string4)
        self.assertTrue(gene4.validate())
        self.assertEqual(gene4.dimension, '*')
        string5 = 'add(faceless, blackhole)'
        gene5 = self.env.parse_string(string5)
        self.assertTrue(gene5.validate())
        self.assertEqual(gene5.dimension, '*')
        string6 = 'add(faceless, faceless)'
        gene6 = self.env.parse_string(string6)
        self.assertTrue(gene6.validate())
        self.assertEqual(gene6.dimension, '?')
        string7 = 'add(blackhole, blackhole)'
        gene7 = self.env.parse_string(string7)
        self.assertTrue(gene7.validate())
        self.assertEqual(gene7.dimension, '*')
        df = self.env.get_data_value('blackhole')
        self.assertTrue(
                frame_equal(
                    gene7.eval(self.env, self.date1, self.date2),
                    df+df)
                )

    def test_sub(self):
        self.env.add_operator('sub', {
            'operator': OperatorSub,
            })
        string1 = 'sub(open1, open2)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'sub(open1, close1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY')
        string3 = 'sub(open1, faceless)'
        gene3 = self.env.parse_string(string3)
        self.assertTrue(gene3.validate())
        self.assertEqual(gene3.dimension, 'CNY')
        string4 = 'sub(open1, blackhole)'
        gene4 = self.env.parse_string(string4)
        self.assertTrue(gene4.validate())
        self.assertEqual(gene4.dimension, '*')
        string5 = 'sub(faceless, blackhole)'
        gene5 = self.env.parse_string(string5)
        self.assertTrue(gene5.validate())
        self.assertEqual(gene5.dimension, '*')
        string6 = 'sub(faceless, faceless)'
        gene6 = self.env.parse_string(string6)
        self.assertTrue(gene6.validate())
        self.assertEqual(gene6.dimension, '?')
        string7 = 'sub(blackhole, blackhole)'
        gene7 = self.env.parse_string(string7)
        self.assertTrue(gene7.validate())
        self.assertEqual(gene7.dimension, '*')
        self.assertTrue(
                almost_zero(gene7.eval(self.env, self.date1, self.date2))
                )

    def test_mul(self):
        self.env.add_operator('mul', {
            'operator': OperatorMul,
            })
        string1 = 'mul(open1, open2)'
        gene1 = self.env.parse_string(string1)
        self.assertTrue(gene1.validate())
        self.assertEqual(gene1.dimension, 'CNY USD')
        string2 = 'mul(open1, close1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY**2')
        string3 = 'mul(open1, faceless)'
        gene3 = self.env.parse_string(string3)
        self.assertTrue(gene3.validate())
        self.assertEqual(gene3.dimension, 'CNY')
        string4 = 'mul(open1, blackhole)'
        gene4 = self.env.parse_string(string4)
        self.assertTrue(gene4.validate())
        self.assertEqual(gene4.dimension, '*')
        string5 = 'mul(faceless, blackhole)'
        gene5 = self.env.parse_string(string5)
        self.assertTrue(gene5.validate())
        self.assertEqual(gene5.dimension, '*')
        string6 = 'mul(faceless, faceless)'
        gene6 = self.env.parse_string(string6)
        self.assertTrue(gene6.validate())
        self.assertEqual(gene6.dimension, '?')
        string7 = 'mul(blackhole, blackhole)'
        gene7 = self.env.parse_string(string7)
        self.assertTrue(gene7.validate())
        self.assertEqual(gene7.dimension, '*')
        df = self.env.get_data_value('blackhole')
        self.assertTrue(
                frame_equal(
                    gene7.eval(self.env, self.date1, self.date2),
                    df*df)
                )

    def test_div(self):
        self.env.add_operator('div', {
            'operator': OperatorDiv,
            })
        string1 = 'div(open1, open2)'
        gene1 = self.env.parse_string(string1)
        self.assertTrue(gene1.validate())
        self.assertEqual(gene1.dimension, 'CNY // USD')
        string2 = 'div(open1, close1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, '')
        string3 = 'div(open1, faceless)'
        gene3 = self.env.parse_string(string3)
        self.assertTrue(gene3.validate())
        self.assertEqual(gene3.dimension, 'CNY')
        string4 = 'div(open1, blackhole)'
        gene4 = self.env.parse_string(string4)
        self.assertTrue(gene4.validate())
        self.assertEqual(gene4.dimension, '*')
        string5 = 'div(faceless, blackhole)'
        gene5 = self.env.parse_string(string5)
        self.assertTrue(gene5.validate())
        self.assertEqual(gene5.dimension, '*')
        string6 = 'div(faceless, faceless)'
        gene6 = self.env.parse_string(string6)
        self.assertTrue(gene6.validate())
        self.assertEqual(gene6.dimension, '?')
        string7 = 'div(blackhole, blackhole)'
        gene7 = self.env.parse_string(string7)
        self.assertTrue(gene7.validate())
        self.assertEqual(gene7.dimension, '*')
        self.assertTrue(
                almost_equal(gene7.eval(self.env, self.date1, self.date2), 1)
                )

    def test_exp(self):
        self.env.add_operator('exp', {
            'operator': OperatorExp,
            })
        string1 = 'exp(1, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertEqual(gene1.dimension, 'CNY')
        string2 = 'exp(2, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertEqual(gene2.dimension, 'CNY**2')
        string3 = 'exp(2, faceless)'
        gene3 = self.env.parse_string(string3)
        self.assertEqual(gene3.dimension, '?')
        string4 = 'exp(2, blackhole)'
        gene4 = self.env.parse_string(string4)
        self.assertEqual(gene4.dimension, '*')
        df = self.env.get_data_value('blackhole')
        self.assertTrue(
                frame_equal(
                    gene4.eval(self.env, self.date1, self.date2),
                    df ** 2,
                    check_less_precise=True
                    )
                )

    def test_signedexp(self):
        self.env.add_operator('signed_exp', {
            'operator': OperatorSignedExp,
            })
        string1 = 'signed_exp(1, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertEqual(gene1.dimension, 'CNY')
        string2 = 'signed_exp(2, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertEqual(gene2.dimension, 'CNY**2')
        string3 = 'signed_exp(2, faceless)'
        gene3 = self.env.parse_string(string3)
        self.assertEqual(gene3.dimension, '?')
        string4 = 'signed_exp(2, blackhole)'
        gene4 = self.env.parse_string(string4)
        self.assertEqual(gene4.dimension, '*')
        df = self.env.get_data_value('blackhole')
        self.assertTrue(
                frame_equal(
                    gene4.eval(self.env, self.date1, self.date2),
                    (df ** 2) * np.sign(df))
                )

    def test_abs(self):
        self.env.add_operator('abs', {
            'operator': OperatorAbs,
            })
        string1 = 'abs(open1)'
        gene1 = self.env.parse_string(string1)
        self.assertEqual(gene1.dimension, 'CNY')
        string2 = 'abs(faceless)'
        gene2 = self.env.parse_string(string2)
        self.assertEqual(gene2.dimension, '?')
        string3 = 'abs(blackhole)'
        gene3 = self.env.parse_string(string3)
        self.assertEqual(gene3.dimension, '*')
        df = self.env.get_data_value('blackhole')
        self.assertTrue(
                frame_equal(
                    gene3.eval(self.env, self.date1, self.date2),
                    df.abs())
                )

    def test_ts_mean(self):
        self.env.add_operator('ts_mean', {
            'operator': OperatorTSMean,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_mean(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_mean(3, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 2)
        df = pd.rolling_mean(self.env.get_data_value('open1'), 3).iloc[2:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_min(self):
        self.env.add_operator('ts_min', {
            'operator': OperatorTSMin,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_min(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_min(3, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 2)
        df = pd.rolling_min(self.env.get_data_value('open1'), 3).iloc[2:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_max(self):
        self.env.add_operator('ts_max', {
            'operator': OperatorTSMax,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_max(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_max(3, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 2)
        df = pd.rolling_max(self.env.get_data_value('open1'), 3).iloc[2:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_median(self):
        self.env.add_operator('ts_median', {
            'operator': OperatorTSMedian,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_median(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_median(3, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 2)
        df = pd.rolling_median(self.env.get_data_value('open1'), 3).iloc[2:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_std(self):
        self.env.add_operator('ts_std', {
            'operator': OperatorTSStd,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_std(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_std(3, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 2)
        df = pd.rolling_std(self.env.get_data_value('open1'), 3).iloc[2:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_var(self):
        self.env.add_operator('ts_var', {
            'operator': OperatorTSVar,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_var(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_var(3, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY**2')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 2)
        df = pd.rolling_var(self.env.get_data_value('open1'), 3).iloc[2:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_skew(self):
        self.env.add_operator('ts_skew', {
            'operator': OperatorTSSkew,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_skew(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_skew(3, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, '')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 2)
        df = pd.rolling_skew(self.env.get_data_value('open1'), 3).iloc[2:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_kurt(self):
        self.env.add_operator('ts_kurt', {
            'operator': OperatorTSKurt,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_kurt(2, open1)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_kurt(5, open1)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, '')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 4)
        df = pd.rolling_kurt(self.env.get_data_value('open1'), 5).iloc[4:]
        self.assertTrue(
                (gene2.eval(self.env, date1, self.date2) == df).values.all()
                )
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_cov(self):
        self.env.add_operator('ts_cov', {
            'operator': OperatorTSCov,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_cov(2, open1, open2)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_cov(5, open1, open2)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, 'CNY USD')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 4)
        df = pd.rolling_cov(self.env.get_data_value('open1'), self.env.get_data_value('open2'), 5).iloc[4:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_ts_corr(self):
        self.env.add_operator('ts_corr', {
            'operator': OperatorTSCorr,
            'arg1': {'value': [3, 5]},
            })
        string1 = 'ts_corr(2, open1, open2)'
        gene1 = self.env.parse_string(string1)
        self.assertFalse(gene1.validate())
        string2 = 'ts_corr(5, open1, open2)'
        gene2 = self.env.parse_string(string2)
        self.assertTrue(gene2.validate())
        self.assertEqual(gene2.dimension, '')
        self.assertRaises(IndexError, gene2.eval, self.env, self.date1, self.date2)
        date1 = self.env.shift_date(self.date1, 4)
        df = pd.rolling_corr(self.env.get_data_value('open1'), self.env.get_data_value('open2'), 5).iloc[4:]
        self.assertTrue(
                frame_equal(
                    gene2.eval(self.env, date1, self.date2),
                    df)
                )

    def test_neutralize(self):
        self.env.add_operator('neut', {
            'operator': OperatorNeutralize,
            })
        string = 'neut(open1)'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.dimension, 'CNY')
        df = gene.eval(self.env, self.date1, self.date2)
        self.assertTrue(
                almost_zero(df.sum(axis=1))
                )

    def test_rank(self):
        self.env.add_operator('rank', {
            'operator': OperatorRank,
            })
        string = 'rank(open1)'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.dimension, '')
        df1 = gene.eval(self.env, self.date1, self.date2)
        df2 = api.rank(self.env.get_data_value('open1'))
        self.assertTrue(
                frame_equal(df1, df2))

    def test_power(self):
        self.env.add_operator('power', {
            'operator': OperatorPower,
            })
        string = 'power(1, open1)'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.dimension, '')
        df1 = gene.eval(self.env, self.date1, self.date2)
        df2 = api.power(self.env.get_data_value('open1'))
        self.assertTrue(
                frame_equal(df1, df2))

    def test_group_neutralize(self):
        self.env.add_operator('group_neut', {
            'operator': OperatorGroupNeutralize,
            })
        string = 'group_neut(group, open1)'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.dimension, 'CNY')
        df = gene.eval(self.env, self.date1, self.date2)
        group = self.env.get_data_value('group')
        self.assertTrue(
                almost_zero(df.groupby(group.iloc[-1], axis=1).apply(lambda x: x.sum(axis=1)))
                )

    def test_group_rank(self):
        self.env.add_operator('group_rank', {
            'operator': OperatorGroupRank,
            })
        string = 'group_rank(group, open1)'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.dimension, '')
        df1 = gene.eval(self.env, self.date1, self.date2)
        data = self.env.get_data_value('open1')
        group = self.env.get_data_value('group').iloc[-1]
        df2 = api.group_rank(data, group)
        self.assertTrue(
                frame_equal(df1, df2)
                )

    def test_group_power(self):
        self.env.add_operator('group_power', {
            'operator': OperatorGroupPower,
            })
        string = 'group_power(1, group, open1)'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.dimension, '')
        df1 = gene.eval(self.env, self.date1, self.date2)
        data = self.env.get_data_value('open1')
        group = self.env.get_data_value('group').iloc[-1]
        df2 = api.group_power(data, group)
        self.assertTrue(
                frame_equal(df1, df2)
                )


if __name__ == '__main__':

    unittest.main()
