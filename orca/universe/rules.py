"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

def identity():
    def func(*args):
        return lambda df: df
    return func

def is_finite():
    def func(*args):
        return lambda df: ~df.isnull()
    return func

def isin(x):
    def func(*args):
        return lambda df: df.isin(x)
    return func

def min_gt(x):
    def func(window):
        return lambda df: pd.rolling_min(df, window) > x
    return func

def min_gte(x):
    def func(window):
        return lambda df: pd.rolling_min(df, window) >= x
    return func

def max_le(x):
    def func(window):
        return lambda df: pd.rolling_max(df, window) < x
    return func

def max_lte(x):
    def func(window):
        return lambda df: pd.rolling_max(df, window) <= x
    return func

def avg_le(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) < x * pd.rolling_count(df, window)
    return func

def avg_lte(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) <= x * pd.rolling_count(df, window)
    return func

def avg_gt(x):
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) > x * pd.rolling_count(df, window)
    return func

def avg_gte(x):
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) >= x * pd.rolling_count(df, window)
    return func

def avg_rank_le(x, ascending=False):
    def func(window):
        return lambda df: \
            (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
            rank(axis=1, ascending=ascending) < x
    return func

def avg_rank_lte(x, ascending=False):
    def func(window):
        return lambda df: \
                (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
                rank(axis=1, ascending=ascending) <= x
    return func

def avg_rank_pct_le(x, ascending=False):
    def func(window):
        return lambda df: \
                (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) < x * 0.01
    return func

def avg_rank_pct_lte(x, ascending=False):
    def func(window):
        return lambda df: \
                (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) <= x * 0.01
    return func

def std_rank_le(x, ascending=False):
    def func(window):
        return lambda df: pd.rolling_std(df.fillna(0), window).rank(axis=1, ascending=ascending) < x
    return func

def std_rank_lte(x, ascending=False):
    def func(window):
        return lambda df: pd.rolling_std(df.fillna(0), window).rank(axis=1, ascending=ascending) <= x
    return func

def std_rank_pct_le(x, ascending=False):
    def func(window):
        return lambda df: \
                (pd.rolling_std(df.fillna(0), window).
                rank(axis=1, ascending=ascending)).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) < x * 0.01
    return func

def std_rank_pct_lte(x, ascending=False):
    def func(window):
        return lambda df: pd.rolling_std(df.fillna(0), window).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) <= x * 0.01
    return func

def sum_rank_le(x, ascending=False):
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window).rank(axis=1, ascending=ascending) < x
    return func

def sum_rank_lte(x, ascending=False):
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window).rank(axis=1, ascending=ascending) <= x
    return func

def sum_rank_pct_le(x, ascending=False):
    def func(window):
        return lambda df: \
                pd.rolling_sum(df.fillna(0), window).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) < x * 0.01
    return func

def sum_rank_pct_lte(x, ascending=False):
    def func(window):
        return lambda df: \
                pd.rolling_sum(df.fillna(0), window).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) <= x * 0.01
    return func

def count_gt(x):
    def func(window):
        return lambda df: pd.rolling_count(df, window) > x
    return func

def count_gte(x):
    def func(window):
        return lambda df: pd.rolling_count(df, window) >= x
    return func

def count_pct_gt(x):
    def func(window):
        return lambda df: pd.rolling_count(df, window) > x * window * 0.01
    return func

def count_pct_gte(x):
    def func(window):
        return lambda df: pd.rolling_count(df, window) >= x * window * 0.01
    return func


"""
Some common rules for string manipulation.
"""

def startswith(x):
    if isinstance(x, str):
        l = len(x)
        return lambda s: s[:l] == x
    else:
        return lambda s: any(s[:len(y)] == y for y in x)

def endsiwth(x):
    if isinstance(x, str):
        l = len(x)
        return lambda s: s[-l:] == x
    else:
        return lambda s: any(s[-len(y):] == y for y in x)
