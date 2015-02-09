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
    if isinstance(df, pd.DataFrame):
        df = df.reindex(columns=SIDS, copy=True).fillna(value)
        df.index = pd.to_datetime(df.index)
        return df
    return df.reindex(SIDS)

def intersect(df, univ):
    """Intersect DataFrame with a universe."""
    univ = univ.ix[df.index].fillna(method='bfill').fillna(method='ffill')
    df = df.reindex(columns=univ.columns)
    return df[univ]

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
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        return df.subtract(df.mean(axis=1), axis=0)
    return df - df.mean()

def scale(df):
    """Make DataFrame with absolute sum 1 for each row."""
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        return df.div(np.abs(df).sum(axis=1), axis=0)
    return df / np.abs(df).sum()

def rank(df):
    """Transform each row to be distributed uniformly in [0, 1]."""
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        rdf = df.rank(axis=1)
        rdf = rdf.sub(rdf.min(axis=1), axis=0)
        return rdf.div(rdf.max(axis=1), axis=0)
    rdf = df.rank()
    return (rdf - rdf.min()) / (rdf.max() - rdf.min())

def top(df, n):
    """Return top n elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        return df.rank(ascending=False, axis=1) <= n
    return df.rank(ascending=False) <= n

def bottom(df, n):
    """Return bottom n elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        return df.rank(ascending=True, axis=1) <= n
    return df.rank(ascending=False) <= n

def qtop(df, q):
    """Return top q-quantile elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        return df.ge(df.quantile(1-q, axis=1), axis=0)
    return df >= df.quantile(1-q)

def qbottom(df, q):
    """Return bottom q-quantile elements for each row in DataFrame.

    :returns: A boolean DataFrame with same shape as ``df`` with desired element position as True
    """
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        return df.le(df.quantile(q, axis=1), axis=0)
    return df <= df.quantile(q)

def quantiles(df, n):
    """Cut DataFrames into quantiles.

    :returns: A list of ``n`` boolean DataFrames with same shape as ``df`` with desired element position as True, the first one being bottom quantile and last one being top quantile
    """
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        qtls = []
        qs = df.quantile(q=np.linspace(1./n, 1, n), axis=1)
        qs.index = range(1, n+1)
        qtls.append(df.le(qs.ix[1], axis=0))

        for i in range(2, n+1):
            qtls.append(df.le(qs.ix[i], axis=0) & df.gt(qs.ix[i-1], axis=0))
        return qtls
    qtls = []
    qs = df.quantile(q=np.linspace(1./n, 1, n))
    qs.index = range(1, n+1)
    qtls.append(df <= qs.ix[1])
    for i in range(2, n+1):
        qtls.append((df <= qs.ix[i]) & (df > qs.ix[i-1]))
    return qtls

def sigma_cut(df, sigma=3, method='cut'):
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        m, s = df.mean(axis=1), df.std(axis=1)
        m, s = pd.DataFrame({c: m for c in df.columns}), pd.DataFrame({c: s for c in df.columns})
        l, u = (df <= (m-sigma*s)), (df >= (m+sigma*s))
    else:
        m, s = df.mean(), df.std()
        l, u = (df <= (m-sigma*s)), (df >= (m+sigma*s))
    if method == 'drop':
        df[l], df[u] = np.nan, np.nan
    elif method == 'drop_lower':
        df[l], df[u] = np.nan, m+sigma*s
    elif method == 'drop_upper':
        df[l], df[u] = m-sigma*s, np.nan
    else:
        df[l], df[u] = m-sigma*s, m+sigma*s
    return df

def quartile_cut(df, multiplier=1.5, method='cut'):
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        lq, uq = df.quantile(0.25, axis=1), df.quantile(0.75, axis=1)
        lq, uq = pd.DataFrame({c: lq for c in df.columns}), pd.DataFrame({c: uq for c in df.columns})
        iqr = uq -lq
        l, u = (df <= (lq-multiplier*iqr)), (df >= (uq+multiplier*iqr))
    else:
        lq, uq = df.quantile(0.25), df.quantile(0.75)
        iqr = uq -lq
        l, u = (df <= (lq-multiplier*iqr)), (df >= (uq+multiplier*iqr))
    if method == 'drop':
        df[l], df[u] = np.nan, np.nan
    elif method == 'drop_lower':
        df[l], df[u] = np.nan, uq+multiplier*iqr
    elif method == 'drop_upper':
        df[l], df[u] = lq-multiplier*iqr, np.nan
    else:
        df[l], df[u] = lq-multiplier*iqr, uq+multiplier*iqr
    return df

def quantile_cut(df, lower=0, upper=1, reverse=False):
    df = df[df.notnull()]
    if isinstance(df, pd.DataFrame):
        lq, uq = df.quantile(lower, axis=1), df.quantile(upper, axis=1)
        lq, uq = pd.DataFrame({c: lq for c in df.columns}), pd.DataFrame({c: uq for c in df.columns})
    else:
        lq, uq = df.quantile(lower), df.quantile(upper)
    l, u = (df < lq), (df > uq)
    if not reverse:
        df[l], df[u] = np.nan, np.nan
    else:
        df[~(l|u)] = np.nan
    return df

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
        BoardNeutOperation,
        )
from rank import (
        GroupRankOperation,
        IndustryRankOperation,
        BoardRankOperation,
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

def board_neut(df):
    """Wrapper for :py:class:`orca.operation.rank.BoardNeutOperation`."""
    return BoardNeutOperation().operate(df)

def industry_neut(df, group, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:class:`orca.operation.neutralize.IndustryNeutOperation`.

    :param str group: 'level1', 'level2', 'level3', 'board'
    :param str standard: Industry classification standard, currently only supports: ('SW2014', 'ZX')
    """
    if group == 'board':
        return board_neut(df)
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

def group_rank(df, group=None, date=None):
    """Wrapper for :py:class:`orca.operation.rank.GroupRankOperation`."""
    return GroupRankOperation(group).operate(df, date=date)

def board_rank(df):
    """Wrapper for :py:class:`orca.operation.rank.BoardRankOperation`."""
    return BoardRankOperation().operate(df)

def industry_rank(df, group, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:class:`orca.operation.rank.IndustryRankOperation`.

    :param str group: 'level1', 'level2', 'level3', 'board'
    :param str standard: Industry classification standard, currently only supports: ('SW2014', 'ZX')
    """
    if group == 'board':
        return board_rank(df)
    return IndustryRankOperation(standard).operate(df, group, simple=simple, date=date)

def level1_rank(df, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:func:`orca.operation.api.industry_rank` with ``group='level1'``."""
    return industry_rank(df, 'level1', standard=standard, simple=simple, date=date)

def level2_rank(df, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:func:`orca.operation.api.industry_rank` with ``group='level2'``."""
    return industry_rank(df, 'level2', standard=standard, simple=simple, date=date)

def level3_rank(df, standard='SW2014', simple=False, date=None):
    """Wrapper for :py:func:`orca.operation.api.industry_rank` with ``group='level3'``."""
    return industry_rank(df, 'level3', standard=standard, simple=simple, date=date)
