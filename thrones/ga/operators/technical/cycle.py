from itertools import product

import numpy as np
import pandas as pd
import talib

from ...node import (
        Operator,
        OperatorWithDefaultConstants,
        )
from ...dimension import Dimension


class OperatorHT_DCPERIOD(Operator):

    def __init__(self, window, name, **kwargs):
        self.window = window
        super(OperatorHT_DCPERIOD, self).__init__(name, 1, **kwargs)

    def eval(self, environment, gene, date1, date2):
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.HT_DCPERIOD(df.values[i: i+self.window, j])[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorHT_DCPHASE(Operator):

    def __init__(self, window, name, **kwargs):
        self.window = window
        super(OperatorHT_DCPHASE, self).__init__(name, 1, **kwargs)

    def eval(self, environment, gene, date1, date2):
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.HT_DCPHASE(df.values[i: i+self.window, j])[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorHT_PHASOR(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': [0, 1]}}
        super(OperatorHT_PHASOR, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.HT_PHASOR(df.values[i: i+self.window, j])[return_type][-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorHT_SINE(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': [0, 1]}}
        super(OperatorHT_SINE, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.HT_SINE(df.values[i: i+self.window, j])[return_type][-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorHT_TRENDMODE(Operator):

    def __init__(self, window, name, **kwargs):
        self.window = window
        super(OperatorHT_TRENDMODE, self).__init__(name, 1, **kwargs)

    def eval(self, environment, gene, date1, date2):
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.HT_TRENDMODE(df.values[i: i+self.window, j])[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()
