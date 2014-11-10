"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import SIDS

from decay import DecayOperation

def format(df, value=np.nan):
    df = df.reindex(columns=SIDS, copy=True).fillna(value)
    df.index = pd.to_datetime(df.index)
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

def decay(df, n, dense=False):
    return DecayOperation(n, dense=dense).operate(df)
