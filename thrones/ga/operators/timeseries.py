from abc import abstractmethod

import pandas as pd

from ..node import (
        SameDimensionOutput,
        NoDimensionOutput,
        BinaryOperator,
        TernaryOperator,
        )


class OperatorTSWithOneData(BinaryOperator):

    @abstractmethod
    def ts_operation(df, n):
        raise NotImplementedError

    def eval(self, environment, gene, date1, date2):
        n = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(n-1), -1)
        df = gene.next_value(environment, date1, date2)
        return self.ts_operation(df, n).iloc[n-1:]


class OperatorTSWithTwoData(TernaryOperator):

    @abstractmethod
    def ts_operation(df1, df2, n):
        raise NotImplementedError

    def eval(self, environment, gene, date1, date2):
        n = int(gene.next_value(environment, date1, date2))
        date1 = environment.shift_date(date1, -(n-1), -1)
        df1 = gene.next_value(environment, date1, date2)
        df2 = gene.next_value(environment, date1, date2)
        return self.ts_operation(df1, df2, n).iloc[n-1:]


class SameDimensionOutputOperatorTSWithOneData(SameDimensionOutput, OperatorTSWithOneData):

    def __init__(self, name, **kwargs):
        SameDimensionOutput.__init__(self, 1)
        OperatorTSWithOneData.__init__(self, name, **kwargs)


class NoDimensionOutputOperatorTSWithOneData(NoDimensionOutput, OperatorTSWithOneData):

    def __init__(self, name, **kwargs):
        NoDimensionOutput.__init__(self)
        OperatorTSWithOneData.__init__(self, name, **kwargs)


class NoDimensionOutputOperatorTSWithTwoData(NoDimensionOutput, OperatorTSWithTwoData):

    def __init__(self, name, **kwargs):
        NoDimensionOutput.__init__(self)
        OperatorTSWithTwoData.__init__(self, name, **kwargs)


class OperatorTSMean(SameDimensionOutputOperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_mean(df, n)


class OperatorTSMin(SameDimensionOutputOperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_min(df, n)


class OperatorTSMax(SameDimensionOutputOperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_max(df, n)


class OperatorTSMedian(SameDimensionOutputOperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_median(df, n)


class OperatorTSStd(SameDimensionOutputOperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_std(df, n)


class OperatorTSVar(OperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_var(df, n)

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[1].dimension ** 2


class OperatorTSSkew(NoDimensionOutputOperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_skew(df, n)


class OperatorTSKurt(NoDimensionOutputOperatorTSWithOneData):

    @staticmethod
    def ts_operation(df, n):
        return pd.rolling_kurt(df, n)


class OperatorTSCov(OperatorTSWithTwoData):

    @staticmethod
    def ts_operation(df1, df2, n):
        return pd.rolling_cov(df1, df2, n)

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[1].dimension * children[2].dimension


class OperatorTSCorr(NoDimensionOutputOperatorTSWithTwoData):

    @staticmethod
    def ts_operation(df1, df2, n):
        return pd.rolling_corr(df1, df2, n)
