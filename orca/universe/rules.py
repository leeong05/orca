"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

def identity():
    def func(*args):
        return lambda df: df
    return func

def min_gt(x):
    def func(window):
        return lambda df: pd.rolling_min(df, window) < x
    return func

def min_gte(x):
    def func(window):
        return lambda df: pd.rolling_min(df, window) <= x
    return func

def max_le(x):
    def func(window):
        return lambda df: pd.rolling_max(df, window) > x
    return func

def max_lte(x):
    def func(window):
        return lambda df: pd.rolling_max(df, window) >= x
    return func

def avg_le(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_mean(df, window) < x
    return func

def avg_lte(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_mean(df, window) <= x
    return func

def avg_gt(x):
    def func(window):
        return lambda df: pd.rolling_mean(df, window) > x
    return func

def avg_gte(x):
    def func(window):
        return lambda df: pd.rolling_mean(df, window) >= x
    return func

def avg_rank_le(x):
    def func(window):
        return lambda df: pd.rolling_mean(df, window).rank(axis=1, ascending=False) < x
    return func

def avg_rank_lte(x):
    def func(window):
        return lambda df: pd.rolling_mean(df, window).rank(axis=1, ascending=False) <= x
    return func

def avg_rank_pct_le(x):
    def func(window):
        return lambda df: pd.rolling_mean(df, window).rank(axis=1, ascending=False, pct=True) < x
    return func

def avg_rank_pct_lte(x):
    def func(window):
        return lambda df: pd.rolling_mean(df, window).rank(axis=1, ascending=False, pct=True) <= x
    return func

def std_rank_le(x):
    def func(window):
        return lambda df: pd.rolling_std(df, window).rank(axis=1, ascending=False) < x
    return func

def std_rank_lte(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_std(df, window).rank(axis=1, ascending=False) <= x
    return func

def std_rank_pct_le(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_std(df, window).rank(axis=1, ascending=False, pct=True) < x
    return func

def std_rank_pct_lte(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_std(df, window).rank(axis=1, ascending=False, pct=True) <= x
    return func

def sum_rank_le(x):
    def func(window):
        return lambda df: pd.rolling_sum(df, window).rank(axis=1, ascending=False) < x
    return func

def sum_rank_lte(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_sum(df, window).rank(axis=1, ascending=False) <= x
    return func

def sum_rank_pct_le(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_sum(df, window).rank(axis=1, ascending=False, pct=True) < x
    return func

def sum_rank_pct_lte(x, **kwargs):
    def func(window):
        return lambda df: pd.rolling_sum(df, window).rank(axis=1, ascending=False, pct=True) <= x
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
        return lambda df: pd.rolling_count(df, window) > x * window
    return func

def count_pct_gte(x):
    def func(window):
        return lambda df: pd.rolling_count(df, window) >= x * window
    return func


"""
Some common rules for string manipulation.
"""

def startswith(x):
    l = len(x)
    return lambda s: s[:l] == x

def endsiwth(x):
    l = len(x)
    return lambda s: s[-l:] == x
