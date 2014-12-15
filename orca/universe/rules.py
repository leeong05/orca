"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

def identity():
    """Returns a function that returns the identity function."""
    def func(*args):
        return lambda df: df
    return func

def is_finite():
    """Returns a function that returns a function to check finiteness."""
    def func(*args):
        return lambda df: np.isfinite(df)
    return func

def isin(x):
    """Returns a function that returns a function to check if element is contained in the given list."""
    def func(*args):
        return lambda df: df.isin(x)
    return func

def avg_abs_lt(x):
    """Returns a function that returns a function to check if the absolute value of rolling_mean < x."""
    def func(window):
        return lambda df: np.abs(pd.rolling_mean(df, window)) < x
    return func

def avg_abs_lte(x):
    """Returns a function that returns a function to check if the absolute value of rolling_mean <= x."""
    def func(window):
        return lambda df: np.abs(pd.rolling_mean(df, window)) <= x
    return func

def min_gt(x):
    """Returns a function that returns a function to check if rolling minimal > x."""
    def func(window):
        return lambda df: pd.rolling_min(df, window) > x
    return func

def min_gte(x):
    """Returns a function that returns a function to check if rolling minimal >= x."""
    def func(window):
        return lambda df: pd.rolling_min(df, window) >= x
    return func

def max_le(x):
    """Returns a function that returns a function to check if rolling maximal < x."""
    def func(window):
        return lambda df: pd.rolling_max(df, window) < x
    return func

def max_lte(x):
    """Returns a function that returns a function to check if rolling maximal <= x."""
    def func(window):
        return lambda df: pd.rolling_max(df, window) <= x
    return func

def avg_lt(x, **kwargs):
    """Returns a function that returns a function to check if rolling mean < x."""
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) < x * pd.rolling_count(df, window)
    return func

def avg_lte(x, **kwargs):
    """Returns a function that returns a function to check if rolling mean <= x."""
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) <= x * pd.rolling_count(df, window)
    return func

def avg_gt(x):
    """Returns a function that returns a function to check if rolling mean > x."""
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) > x * pd.rolling_count(df, window)
    return func

def avg_gte(x):
    """Returns a function that returns a function to check if rolling mean >= x."""
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window) >= x * pd.rolling_count(df, window)
    return func

def avg_rank_lt(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank of rolling mean < x."""
    def func(window):
        return lambda df: \
            (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
            rank(axis=1, ascending=ascending) < x
    return func

def avg_rank_lte(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank of rolling mean <= x."""
    def func(window):
        return lambda df: \
                (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
                rank(axis=1, ascending=ascending) <= x
    return func

def avg_rank_pct_lt(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank percentage of rolling mean < x."""
    def func(window):
        return lambda df: \
                (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) < x * 0.01
    return func

def avg_rank_pct_lte(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank percentage of rolling mean <= x."""
    def func(window):
        return lambda df: \
                (pd.rolling_sum(df.fillna(0), window)/pd.rolling_count(df, window)).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) <= x * 0.01
    return func

def std_rank_lt(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank of rolling standard deviation < x."""
    def func(window):
        return lambda df: pd.rolling_std(df.fillna(0), window).rank(axis=1, ascending=ascending) < x
    return func

def std_rank_lte(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank of rolling standard deviation <= x."""
    def func(window):
        return lambda df: pd.rolling_std(df.fillna(0), window).rank(axis=1, ascending=ascending) <= x
    return func

def std_rank_pct_lt(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank percentage of rolling standard deviation < x."""
    def func(window):
        return lambda df: \
                (pd.rolling_std(df.fillna(0), window).
                rank(axis=1, ascending=ascending)).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) < x * 0.01
    return func

def std_rank_pct_lte(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank percentage of rolling standard deviation <= x."""
    def func(window):
        return lambda df: pd.rolling_std(df.fillna(0), window).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) <= x * 0.01
    return func

def sum_rank_lt(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank of rolling sum < x."""
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window).rank(axis=1, ascending=ascending) < x
    return func

def sum_rank_lte(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank of rolling sum <= x."""
    def func(window):
        return lambda df: pd.rolling_sum(df.fillna(0), window).rank(axis=1, ascending=ascending) <= x
    return func

def sum_rank_pct_lt(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank percentage of rolling sum < x."""
    def func(window):
        return lambda df: \
                pd.rolling_sum(df.fillna(0), window).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) < x * 0.01
    return func

def sum_rank_pct_lte(x, ascending=False):
    """Returns a function that returns a function to check if the cross-section rank percentage of rolling sum <= x."""
    def func(window):
        return lambda df: \
                pd.rolling_sum(df.fillna(0), window).\
                rank(axis=1, ascending=ascending).\
                div(df.fillna(method='ffill', limit=window).count(axis=1), axis=0) <= x * 0.01
    return func

def count_gt(x):
    """Returns a function that returns a function to check if the rolling valid number of observations > x."""
    def func(window):
        return lambda df: pd.rolling_count(df, window) > x
    return func

def count_gte(x):
    """Returns a function that returns a function to check if the rolling valid number of observations >= x."""
    def func(window):
        return lambda df: pd.rolling_count(df, window) >= x
    return func

def count_pct_gt(x):
    """Returns a function that returns a function to check if the rolling valid percentage of observations > x."""
    def func(window):
        return lambda df: pd.rolling_count(df, window) > x * window * 0.01
    return func

def count_pct_gte(x):
    """Returns a function that returns a function to check if the rolling valid percentage of observations >= x."""
    def func(window):
        return lambda df: pd.rolling_count(df, window) >= x * window * 0.01
    return func


"""
Some common rules for string manipulation.
"""

def startswith(x):
    """Returns a function that returns a function to check if element has certain prefixes."""
    if isinstance(x, str):
        l = len(x)
        return lambda s: s[:l] == x
    else:
        return lambda s: any(s[:len(y)] == y for y in x)

def endswith(x):
    """Returns a function that returns a function to check if element has certain suffixes."""
    if isinstance(x, str):
        l = len(x)
        return lambda s: s[-l:] == x
    else:
        return lambda s: any(s[-len(y):] == y for y in x)
