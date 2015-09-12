from itertools import product

import numpy as np
import pandas as pd
import talib

from ...node import (
        TernaryOperator,
        OperatorWithDefaultConstants,
        )
from ...dimension import Dimension


class OperatorATR(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorATR, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.ATR(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        dims = set([str(child.dimension) for child in children[1:]])
        if '?' in dims:
            dims.remove('?')
        if len(dims) == 0:
            self.dimension = Dimension.parse('?')
            return
        if '*' in dims:
            dims.remove('*')
            self.dimension = Dimension.parse('*')
            return
        if len(dims) == 1:
            self.dimension = Dimension.parse(dims.keys()[0])
        else:
            self.dimension = Dimension.parse('?')


class OperatorNATR(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorNATR, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.NATR(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorTRANGE(TernaryOperator):

    def eval(self, environment, gene, date1, date2):
        date1 = environment.shift_date(date1, -1, -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        close = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: talib.TRANGE(x.values, low[x.name].values, close[x.name].values))
        return res.iloc[1:]

    def validate(self, gene):
        children = self.validate_children(gene)
        dims = set([str(child.dimension) for child in children])
        if '?' in dims:
            dims.remove('?')
        if len(dims) == 0:
            self.dimension = Dimension.parse('?')
            return
        if '*' in dims:
            dims.remove('*')
            self.dimension = Dimension.parse('*')
            return
        if len(dims) == 1:
            self.dimension = Dimension.parse(dims.keys()[0])
        else:
            self.dimension = Dimension.parse('?')
