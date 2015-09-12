from itertools import product

import numpy as np
import pandas as pd
import talib

from ...node import (
        Operator,
        OperatorWithDefaultConstants,
        )
from ...dimension import Dimension


class OperatorADX(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorADX, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.ADX(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorADXR(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorADXR, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.ADXR(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorAPO(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 12}, 'arg2': {'value': 26}}
        super(OperatorAPO, self).__init__(name, 3, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        fastperiod = int(gene.next_value(environment, date1, date2))
        slowperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(slowperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.APO(
                x.values,
                slowperiod=slowperiod, fastperiod=fastperiod),
            index=df.index))
        return res.iloc[(slowperiod-1): ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorAROON(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': [0, 1]}, 'arg2': {'value': 14}}
        super(OperatorAROON, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -timeperiod, -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.AROON(
                x.values, low[x.name].values,
                timeperiod=timeperiod)[return_type],
            index=high.index))
        return res.iloc[timeperiod: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorAROONOSC(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorAROONOSC, self).__init__(name, 3, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -timeperiod, -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.AROONOSC(
                x.values, low[x.name].values,
                timeperiod=timeperiod),
            index=high.index))
        return res.iloc[timeperiod: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorBOP(Operator):

    def __init__(self, name, **kwargs):
        super(OperatorBOP, self).__init__(name, 4, **kwargs)

    def eval(self, environment, gene, date1, date2):
        o = gene.next_value(environment, date1, date2)
        h = gene.next_value(environment, date1, date2)
        l = gene.next_value(environment, date1, date2)
        c = gene.next_value(environment, date1, date2)
        res = o.apply(lambda x: pd.Series(
            talib.BOP(
                x.values, h[x.name].values, l[x.name].values, c[x.name].values),
            index=o.index))
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorCCI(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorCCI, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        close = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.CCI(
                x.values, low[x.name].values, close[x.name].values,
                timeperiod=timeperiod),
            index=high.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorCMO(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorCMO, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.CMO(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorDX(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorDX, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.DX(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorMACD(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {
                'arg1': {'value': [0, 1, 2]},
                'arg2': {'value': 12},
                'arg3': {'value': 26},
                'arg4': {'value': 9},
                }
        super(OperatorMACD, self).__init__(name, 5, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        fastperiod = int(gene.next_value(environment, date1, date2))
        slowperiod = int(gene.next_value(environment, date1, date2))
        signalperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.MACD(
                    df.values[i: i+self.window, j],
                    fastperiod=fastperiod, slowperiod=slowperiod,signalperiod=signalperiod)[return_type][-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorMACDEXT(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {
                'arg1': {'value': [0, 1, 2]},
                'arg2': {'value': 12},
                'arg3': {'value': 26},
                'arg4': {'value': 9},
                }
        super(OperatorMACDEXT, self).__init__(name, 5, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        fastperiod = int(gene.next_value(environment, date1, date2))
        slowperiod = int(gene.next_value(environment, date1, date2))
        signalperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(slowperiod-1+signalperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.MACDEXT(
                x.values,
                fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)[return_type],
            index=df.index))
        return res.iloc[slowperiod-1+signalperiod-1: ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorMFI(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorMFI, self).__init__(name, 5, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -timeperiod, -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        close = gene.next_value(environment, date1, date2)
        volume = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.MFI(
                x.values, low[x.name].values, close[x.name].values, volume[x.name].values,
                timeperiod=timeperiod),
            index=high.index))
        return res.iloc[timeperiod: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorMINUS_DI(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorMINUS_DI, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.MINUS_DI(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorMINUS_DM(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorMINUS_DM, self).__init__(name, 3, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.MINUS_DM(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        if children[1].dimension == children[2].dimension:
            self.dimension = children[1].dimension.copy()
        else:
            self.dimension = Dimension.parse('?')


class OperatorMOM(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 10}}
        super(OperatorMOM, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -timeperiod, -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.MOM(
                x.values,
                timeperiod=timeperiod),
            index=df.index))
        return res.iloc[timeperiod: ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorPLUS_DI(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorPLUS_DI, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        close = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.PLUS_DI(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j], close.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorPLUS_DM(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorPLUS_DM, self).__init__(name, 3, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        high = gene.next_value(environment, date1_, date2)
        low = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=high.ix[date1:date2].index, columns=high.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.PLUS_DM(
                    high.values[i: i+self.window, j], low.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        children = self.validate_children(gene)
        if children[1].dimension == children[2].dimension:
            self.dimension = children[1].dimension.copy()
        else:
            self.dimension = Dimension.parse('?')


class OperatorPPO(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 12}, 'arg2': {'value': 26}}
        super(OperatorPPO, self).__init__(name, 3, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        fastperiod = int(gene.next_value(environment, date1, date2))
        slowperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(slowperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.PPO(
                x.values,
                fastperiod=fastperiod, slowperiod=slowperiod),
            index=df.index))
        return res.iloc[slowperiod-1: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorRSI(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {'arg1': {'value': 14}}
        super(OperatorRSI, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1_ = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1_, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.RSI(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod)[-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorSTOCH(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {
                'arg1': {'value': [0, 1]},
                'arg2': {'value': 5},
                'arg3': {'value': 3},
                'arg4': {'value': 3},
                }
        super(OperatorSTOCH, self).__init__(name, 7, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        fastk_period = int(gene.next_value(environment, date1, date2))
        slowk_period = int(gene.next_value(environment, date1, date2))
        slowd_period = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(fastk_period-1+slowk_period-1+slowd_period-1), -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        close = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.STOCH(
                x.values, low[x.name].values, close[x.name].values,
                fastk_period=fastk_period, slowk_period=slowk_period, slowd_period=slowd_period)[return_type],
            index=high.index))
        return res.iloc[fastk_period-1+slowk_period-1+slowd_period-1: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorSTOCHF(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {
                'arg1': {'value': [0, 1]},
                'arg1': {'value': 5},
                'arg2': {'value': 3},
                }
        super(OperatorSTOCHF, self).__init__(name, 6, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        fastk_period = int(gene.next_value(environment, date1, date2))
        fastd_period = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(fastk_period-1+fastd_period-1), -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        close = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.STOCHF(
                x.values, low[x.name].values, close[x.name].values,
                fastk_period=fastk_period, fastd_period=fastd_period)[return_type],
            index=high.index))
        return res.iloc[fastk_period-1+fastd_period-1: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorSTOCHRSI(OperatorWithDefaultConstants):

    def __init__(self, window, name, **kwargs):
        self.window = window
        default = {
                'arg1': {'value': [0, 1]},
                'arg2': {'value': 14},
                'arg3': {'value': 5},
                'arg4': {'value': 3},
                }
        super(OperatorSTOCHRSI, self).__init__(name, 5, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        return_type = int(gene.next_value(environment, date1, date2))
        timeperiod = int(gene.next_value(environment, date1, date2))
        fastk_period = int(gene.next_value(environment, date1, date2))
        fastd_period = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(self.window-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = pd.DataFrame(np.nan, index=df.ix[date1:date2].index, columns=df.columns)
        for i, j in product(range(res.shape[0]), range(res.shape[1])):
            res.iloc[i, j] = talib.STOCHRSI(
                    df.values[i: i+self.window, j],
                    timeperiod=timeperiod, fastk_period=fastk_period, fastd_period=fastd_period)[return_type][-1]
        return res

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorULTOSC(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {
                'arg1': {'value': 7},
                'arg2': {'value': 14},
                'arg3': {'value': 28},
                }
        super(OperatorULTOSC, self).__init__(name, 6, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod1 = int(gene.next_value(environment, date1, date2))
        timeperiod2 = int(gene.next_value(environment, date1, date2))
        timeperiod3 = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -timeperiod3, -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        close = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.ULTOSC(
                x.values, low[x.name].values, close[x.name].values,
                timeperiod1=timeperiod1, timeperiod2=timeperiod2, timeperiod3=timeperiod3),
            index=high.index))
        return res.iloc[timeperiod3: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorWILLR(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorWILLR, self).__init__(name, 4, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        high = gene.next_value(environment, date1, date2)
        low = gene.next_value(environment, date1, date2)
        close = gene.next_value(environment, date1, date2)
        res = high.apply(lambda x: pd.Series(
            talib.WILLR(
                x.values, low[x.name].values, close[x.name].values,
                timeperiod=timeperiod),
            index=high.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()
