from itertools import product

import numpy as np
import pandas as pd
import talib

from ...node import (
        Operator,
        OperatorWithDefaultConstants,
        )
from ...dimension import Dimension


class OperatorLINEARREG(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorLINEARREG, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = (gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.LINEARREG(x.values, timeperiod=timeperiod),
            index=df.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorLINEARREG_ANGLE(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorLINEARREG_ANGLE, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = (gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.LINEARREG_ANGLE(x.values, timeperiod=timeperiod),
            index=df.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorLINEARREG_INTERCEPT(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorLINEARREG_INTERCEPT, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = (gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.LINEARREG_INTERCEPT(x.values, timeperiod=timeperiod),
            index=df.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorLINEARREG_SLOPE(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorLINEARREG_SLOPE, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = (gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.LINEARREG_SLOPE(x.values, timeperiod=timeperiod),
            index=df.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()


class OperatorTSF(OperatorWithDefaultConstants):

    def __init__(self, name, **kwargs):
        default = {'arg1': {'value': 14}}
        super(OperatorTSF, self).__init__(name, 2, default=default, **kwargs)

    def eval(self, environment, gene, date1, date2):
        timeperiod = (gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(timeperiod-1), -1)
        df = gene.next_value(environment, date1, date2)
        res = df.apply(lambda x: pd.Series(
            talib.TSF(x.values, timeperiod=timeperiod),
            index=df.index))
        return res.iloc[timeperiod-1: ]

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[-1].dimension.copy()
