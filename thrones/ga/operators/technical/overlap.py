from itertools import product

import numpy as np
import pandas as pd
import talib

from ...node import (
        Operator,
        OperatorWithDefaultConstants,
        )
from ...dimension import Dimension


class OperatorBBANDS(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {
                'arg1': {'value': [0, 1, 2]},
                'arg2': {'value': 5},
                'arg3': {'value': 2},
                'arg4': {'value': 2},
                }
        super(OperatorBBANDS, self).__init__(name, 5, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        timeperiod = int(gene.next_value(environment, date1, date2))
        nbdevup = int(gene.next_value(environment, date1, date2))
        nbdevdn = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.BBANDS(x.values,
                timeperiod=timeperiod,
                nbdevup=nbdevup,
                nbdevdn=nbdevdn)[return_type],
            index=x.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorDEMA(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 30}}
        super(OperatorDEMA, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.DEMA(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorEMA(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 30}}
        super(OperatorEMA, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.EMA(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorHT_TRENDLINE(Operator):

    def __init__(self, window, name, **kwargs):
        self.window = window
        super(OperatorHT_TRENDLINE, self).__init__(name, 1, **kwargs)

    def eval(self, environment, gene, date1, date2):
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.HT_TRENDLINE(df.values[i: i+self.window, j])[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[0].dimension.copy()


class OperatorKAMA(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 30}}
        super(OperatorKAMA, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.KAMA(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorMAMA(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {
                'arg1': {'value': [0, 1]},
                'arg2': {'value': 0.5},
                'arg3': {'value': 0.05},
                }
        super(OperatorMAMA, self).__init__(name, 3, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        fastlimit = float(gene.next_value(environment, date1, date2))
        slowlimit = float(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.MAMA(
                    df.values[i: i+self.window, j],
                    fastlimit=fastlimit, slowlimit=slowlimit)[return_type][-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorSAR(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 0.02}, 'arg2': {'value': 0.2}}
        super(OperatorSAR, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        acceleration = float(gene.next_value(environment, date1, date2))
        maximum = float(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.SAR(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j],
                    acceleration=acceleration, maximum=maximum)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        dims = set([str(child.dimension) for child in children[-2:]])
        if '?' in dims:
            dims.remove('?')
        if len(dims) == 0:
            self.dimension = Dimension.parse('?')
            return
        if '*' in dims:
            dims.remove('*')
            self.dimension = Dimension.parse('*')
        if len(dims) == 1:
            self.dimension = Dimension.parse(dims.pop())
        else:
            self.dimension = Dimension.parse('?')


class OperatorSAREXT(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {
                'arg1': {'value': 0},
                'arg2': {'value': 0},
                'arg3': {'value': 0.02},
                'arg4': {'value': 0.02},
                'arg5': {'value': 0.2},
                'arg6': {'value': 0.02},
                'arg7': {'value': 0.02},
                'arg8': {'value': 0.2},
                }
        super(OperatorSAREXT, self).__init__(name, 10, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        startvalue = float(gene.next_value(environment, date1, date2))
        offsetonreverse = float(gene.next_value(environment, date1, date2))
        accelerationinitlong = float(gene.next_value(environment, date1, date2))
        accelerationlong = float(gene.next_value(environment, date1, date2))
        accelerationmaxlong = float(gene.next_value(environment, date1, date2))
        accelerationinitshort = float(gene.next_value(environment, date1, date2))
        accelerationshort = float(gene.next_value(environment, date1, date2))
        accelerationmaxshort = float(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.SAREXT(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j],
                    startvalue=startvalue, offsetonreverse=offsetonreverse,
                    accelerationinitlong=accelerationinitlong, accelerationlong=accelerationlong, accelerationmaxlong=accelerationmaxlong,
                    accelerationinitshort=accelerationinitshort, accelerationshort=accelerationshort, accelerationmaxshort=accelerationmaxshort)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        dims = set([str(child.dimension) for child in children[-2:]])
        if '?' in dims:
            dims.remove('?')
        if len(dims) == 0:
            self.dimension = Dimension.parse('?')
            return
        if '*' in dims:
            dims.remove('*')
            self.dimension = Dimension.parse('*')
        if len(dims) == 1:
            self.dimension = Dimension.parse(dims.pop())
        else:
            self.dimension = Dimension.parse('?')


class OperatorT3(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 5}, 'arg2': {'value': 0.7}}
        super(OperatorT3, self).__init__(name, 3, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = float(gene.next_value(environment, date1, date2))
        vfactor = float(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.T3(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod, vfactor=vfactor)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorTEMA(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 30}}
        super(OperatorTEMA, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = float(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.TEMA(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorTRIMA(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 30}}
        super(OperatorTRIMA, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.TRIMA(
                x.values,
                timeperiod=timeperiod),
            index=x.index))
        return res.iloc[timeperiod-1:]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorWMA(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 30}}
        super(OperatorWMA, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.WMA(
                x.values,
                timeperiod=timeperiod),
            index=x.index))
        return res.iloc[timeperiod-1:]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()
