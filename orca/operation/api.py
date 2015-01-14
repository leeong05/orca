"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import SIDS

def format(df, value=np.nan):
    """Format DataFrame into DatetimeIndex and full sids columns.

    :param value: Value to be filled for missing points
    """
    df = df.reindex(columns=SIDS, copy=True).fillna(value)
    df.index = pd.to_datetime(df.index)
    return df

def intersect(df, univ):
    """Intersect DataFrame with a universe."""
    df = df.copy()
    df[~(univ.ix[df.index].fillna(False))] = np.nan
    return df

def intersect_interval(df, univ):
    """Intersect an interval DataFrame with a daily universe."""
    dates = np.unique(df.index.date)
    univ = univ.copy().ix[dates]
    index = df.index[::len(df)/len(dates)]
    univ.index = index
    univ = univ.reindex(index=df.index).fillna(method='ffill')
    df = df.copy()
    df[~univ] = np.nan
    return df

def neutralize(df):
    """Make DataFrame with mean 0 for each row."""
    return df.subtract(df.mean(axis=1), axis=0)

def scale(df):
    """Make DataFrame with absolute sum 1 for each row."""
    return df.div(np.abs(df).sum(axis=1), axis=0)

def top(df, n):
    """Return top n elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    return df.rank(ascending=False, axis=1) <= n

def bottom(df, n):
    """Return bottom n elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    return df.rank(ascending=True, axis=1) <= n

def qtop(df, q):
    """Return top q-quantile elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    return df.ge(df.quantile(1-q, axis=1), axis=0)

def qbottom(df, q):
    """Return bottom q-quantile elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    return df.le(df.quantile(q, axis=1), axis=0)

def quantiles(df, n):
    """Cut DataFrames into quantiles.

    :returns: A list of ``n`` boolean DataFrames with same shape as ``df`` with desired element position as True, the first one being bottom quantile and last one being top quantile
    """
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

def decay(df, n, dense=False, exp=1):
    """Wrapper for :py:class:`orca.operation.decay.DecayOperation`."""
    return DecayOperation(n, dense=dense).operate(df, exp=exp)

def barra_neut(df, model, factors):
    """Wrapper for :py:class:`orca.operation.barra.BarraFactorNeutOperation`."""
    return BarraFactorNeutOperation(model).operate(df, factors)

def barra_corr_neut(df, model, factors):
    """Wrapper for :py:class:`orca.operation.barra.BarraFactorCorrNeutOperation`."""
    return BarraFactorCorrNeutOperation(model).operate(df, factors)

def group_neut(df, group=None, date=None):
    """Wrapper for :py:class:`orca.operation.neutralize.GroupNeutOperation`."""
    return GroupNeutOperation(group).operate(df, date=date)

def industry_neut(df, group, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:class:`orca.operation.neutralize.IndustryNeutOperation`.

    :param str group: 'level1', 'level2', 'level3'
    :param str standard: Industry classification standard, currently only supports: ('SW2014', 'ZX')
    """
    return IndustryNeutOperation(standard).operate(df, group, simple=simple, date=date)

def level1_neut(df, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:func:`orca.operation.api.industry_neut` with ``group='level1'``."""
    return industry_neut(df, 'level1', standard=standard, simple=simple, date=date)

def level2_neut(df, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:func:`orca.operation.api.industry_neut` with ``group='level2'``."""
    return industry_neut(df, 'level2', standard=standard, simple=simple, date=date)

def level3_neut(df, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:func:`orca.operation.api.industry_neut` with ``group='level3'``."""
    return industry_neut(df, 'level3', standard=standard, simple=simple, date=date)
