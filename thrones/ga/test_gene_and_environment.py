import unittest
from fractions import Fraction

import numpy as np
import pandas as pd

from ..util.datetime import to_dateint

from .environment import Environment
from .operators import (
        OperatorAdd,
        OperatorMultiply,
        )
from .node import (
        Constant,
        Data,
        Operator,
        )
from .gene import Gene


class MyEnvironment(Environment):

    def set_dates(self):
        self.dates = []

    def add_datas(self):
        self.datas = {
                'open': {'value': 'open', 'dimension': 'CNY'},
                'close': {'value': 'close', 'dimension': 'CNY'},
                }

    def add_operators(self):
        self.operators = {
                'add': {'operator': OperatorAdd},
                'mul': {'operator': OperatorMultiply},
                }

    def set_universe(self):
        self.universe = None

    def set_returns(self):
        self.returns = None
        self.ranked_returns = None


class TestGeneAndEnvironment(unittest.TestCase):

    def setUp(self):
        self.env = MyEnvironment()
        self.dates = to_dateint(pd.date_range('20150101', '20150201', freq='B'))
        self.past_date, self.future_date = 20141231, 20150202
        self.df = pd.DataFrame(np.random.randn(len(self.dates), 5), index=self.dates)

    def test_abc(self):
        self.assertRaises(TypeError, Environment)

    def test_abstractmethods(self):
        env = MyEnvironment()
        del env

    def test_get_frame1(self):
        sdf = self.env.get_frame(self.df, self.dates[0], self.dates[-1])
        self.assertEqual(len(sdf), len(self.dates))

    def test_get_frame2(self):
        self.assertRaises(IndexError, self.env.get_frame, self.df, self.past_date, self.dates[-1])
        self.assertRaises(IndexError, self.env.get_frame, self.df, self.dates[0], self.future_date)

    def test_shift_date(self):
        self.env.dates = self.dates
        date1 = self.env.shift_date(20150201, 0, -1)
        date2 = self.env.shift_date(20150202, 0, -1)
        self.assertEqual(date1, date2)
        date3 = self.env.shift_date(20141231, 0, 1)
        date4 = self.env.shift_date(20150101, 0, 1)
        self.assertEqual(date3, date4)

    def test_create_node(self):
        node1 = self.env.create_node('1/3')
        self.assertIsInstance(node1, Constant)
        node2 = self.env.create_node('close')
        self.assertIsInstance(node2, Data)
        node3 = self.env.create_node('add')
        self.assertIsInstance(node3, Operator)

    def test_parse_string(self):
        gene1 = self.env.parse_string('add(open, close)')
        self.assertIsInstance(gene1, Gene)
        gene2 = self.env.parse_string('add(open, 1/3)')
        self.assertIsInstance(gene2, Gene)

    def test_parse_xmlstring(self):
        gene1 = self.env.parse_string('add(open, add(close, open))')
        gene2 = self.env.parse_xmlstring('<Operator value="add"><Data value="open"/><Operator value="add"><Data value="close"/><Data value="open"/></Operator></Operator>')
        self.assertEqual(gene1, gene2)

    def test_bool(self):
        gene = Gene()
        self.assertFalse(bool(gene))

    def test_length(self):
        gene = self.env.parse_string('add(open, add(close, open))')
        self.assertEqual(len(gene), gene.length)
        self.assertEqual(len(gene), 5)

    def test_depth(self):
        gene = self.env.parse_string('add(open, add(close, open))')
        self.assertEqual(gene.depth, 3)
        gene = self.env.parse_string('add(open, add(add(close, add(close, open)), open))')
        self.assertEqual(gene.depth, 5)

    def test_str(self):
        string = 'add(open, add(close, open))'
        gene = self.env.parse_string(string)
        self.assertEqual(str(gene), string)

    def test_to_xmlstring(self):
        xmlstring = '<Operator value="add"><Data value="open"/><Operator value="add"><Data value="close"/><Data value="open"/></Operator></Operator>'
        gene1 = self.env.parse_xmlstring(xmlstring)
        gene2 = self.env.parse_xmlstring(gene1.to_xmlstring())
        self.assertEqual(gene1.to_xmlstring(), gene2.to_xmlstring())

    def test_dimension(self):
        gene1 = self.env.parse_string('mul(open, close)')
        self.assertEqual(str(gene1.dimension), 'CNY**2')
        gene2 = self.env.parse_string('add(mul(open, close), open)')
        self.assertEqual(str(gene2.dimension), '')
        self.assertFalse(gene2.validate())

    def test_eq_neq(self):
        string1 = 'add(open, add(close, open))'
        string2 = 'add(open, add(open, close))'
        gene1 = self.env.parse_string(string1)
        gene2 = self.env.parse_string(string2)
        gene3 = self.env.parse_string(string1)
        self.assertTrue(gene1 != gene2)
        self.assertTrue(gene1 == gene3)

    def test_next_node(self):
        string = 'add(open, add(close, open))'
        gene = self.env.parse_string(string)
        node1 = gene.next_node()
        self.assertIsInstance(node1, Operator)
        self.assertEqual(str(node1), 'add')
        node2 = gene.next_node()
        self.assertIsInstance(node2, Data)
        self.assertEqual(str(node2), 'open')

    def test_next_sibling(self):
        string = 'add(open, add(close, open))'
        gene = self.env.parse_string(string)
        self.assertEqual(gene, gene.next_sibling())
        self.assertEqual(str(gene.next_sibling(2)), 'add(close, open)')

    def test_copy(self):
        string = 'add(open, add(close, open))'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.copy(), gene.next_sibling())

    def test_next_value(self):
        string = 'add(1, mul(2/3, -1/3))'
        gene = self.env.parse_string(string)
        self.assertEqual(gene.next_value(self.env, 0, 0), Fraction(7, 9))

    def test_eval(self):
        string = 'add(1, mul(2/3, open))'
        gene = self.env.parse_string(string)
        self.assertRaises(TypeError, gene.eval, self.env, 0, 0)

        self.env.datas['open']['value'] = self.df
        string = 'add(1, mul(2/3, open))'
        gene = self.env.parse_string(string)
        df = gene.eval(self.env, self.dates[0], self.dates[-1])
        res = 1 + Fraction(2, 3)*self.df
        self.assertTrue((res == df).values.all())
        self.assertTrue((gene.eval(self.env, self.dates[0], self.dates[-1]) == df).values.all())

    def test_replace(self):
        string = 'add(open, add(close, open))'
        gene = self.env.parse_string(string)
        new_gene = gene.replace(1, gene)
        self.assertEqual(str(new_gene), 'add(add(open, add(close, open)), add(close, open))')


if __name__ == '__main__':

    unittest.main()
