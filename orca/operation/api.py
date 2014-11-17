"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import SIDS

def format(df, value=np.nan):
    df = df.reindex(columns=SIDS, copy=True).fillna(value)
    df.index = pd.to_datetime(df.index)
    return df

def intersect(df, univ):
    df = df.copy()
    df[~univ.ix[df.index]] = np.nan
    return df

def neutralize(df):
    return df.subtract(df.mean(axis=1), axis=0)

def scale(df):
    return df.div(np.abs(df).sum(axis=1), axis=0)

def top(df, n):
    return df.rank(ascending=False, axis=1) <= n

def bottom(df, n):
    return df.rank(ascending=True, axis=1) <= n

def qtop(df, q):
    return df.ge(df.quantile(1-q, axis=1), axis=0)

def qbottom(df, q):
    return df.le(df.quantile(q, axis=1), axis=0)

def quantiles(df, n):
    qtls = []
    qs = df.quantile(q=np.linspace(1./n, 1, n), axis=1)
    qs.index = range(1, n+1)
    qtls.append(df.le(qs.ix[1], axis=0))

    for i in range(2, n+1):
        qtls.append(df.le(qs.ix[i], axis=0) & df.gt(qs.ix[i-1], axis=0))
    return qtls

"""
Helper APIs from operation classes
"""

from decay import DecayOperation
from barra import (
        BarraFactorNeutOperation,
        BarraFactorCorrNeutOperation,
        )
from neutralize import (
        GroupNeutOperation,
        IndustryNeutOperation,
        )

def decay(df, n, dense=False):
    return DecayOperation(n, dense=dense).operate(df)

def barra_neut(df, model, factors):
    return BarraFactorNeutOperation(model).operate(df, factors)

def barra_corr_neut(df, model, factors):
    return BarraFactorCorrNeutOperation(model).operate(df, factors)

def group_neut(df, group=None):
    return GroupNeutOperation(group).operate(df)

def industry_neut(df, group, standard='SW2014'):
    return IndustryNeutOperation(standard).operate(df, group)

def level1_neut(df, standard='SW2014'):
    return industry_neut(df, 'level1', standard=standard)

def level2_neut(df, standard='SW2014'):
    return industry_neut(df, 'level2', standard=standard)

def level3_neut(df, standard='SW2014'):
    return industry_neut(df, 'level3', standard=standard)
