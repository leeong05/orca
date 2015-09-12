from itertools import product

import numpy as np
import pandas as pd
import talib

from ...node import (
        Operator,
        OperatorWithDefaultConstants,
        )


class OperatorAD(Operator):

    def __init__(self, window, name, **kwargs):
        self.window = window
        super(OperatorAD, self).__init__(name, 4, **kwargs)

    def eval(self, environment, gene, date1, date2):
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        volume = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.AD(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j], volume.values[i: i+self.window, j])[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorADOSC(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 3}, 'arg2': {'value': 10}}
        super(OperatorADOSC, self).__init__(name, 6, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        fastperiod = int(gene.next_value(environment, date1, date2))
        slowperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        volume = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.ADOSC(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j], volume.values[i: i+self.window, j],
                    fastperiod=fastperiod, slowperiod=slowperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorOBV(Operator):

    def __init__(self, window, name, **kwargs):
        self.window = window
        super(OperatorOBV, self).__init__(name, 2, **kwargs)

    def eval(self, environment, gene, date1, date2):
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        volume = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.OBV(
                    df.values[i: i+self.window, j], volume.values[i: i+self.window, j])[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()
