"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import DAYS_IN_YEAR

def drawdown(ser):
    """``ser`` is a daily returns Series instead of a cumulative returns Series."""
    ser = ser.cumsum()
    end = (pd.expanding_max(ser)-ser).argmax()
    start = ser.ix[:end].argmax()

    return start, end, (ser[start]-ser[end]) * 100

def annualized_returns(ser):
    """``ser`` is a daily returns Series instead of a cumulative returns Series."""
    return ser.mean() * DAYS_IN_YEAR

def perwin(ser):
    """``ser`` is a daily returns Series instead of a cumulative returns Series."""
    return (ser > 0).sum() * 100. / ser.count()

def IR(ser):
    """``ser`` is a daily returns Series instead of a cumulative returns Series."""
    return ser.mean() / ser.std() if ser.std() > 0 else np.inf * ser.mean()

def Sharpe(ser):
    """``ser`` is a daily returns Series instead of a cumulative returns Series."""
    return ser.mean() / ser.std() * np.sqrt(DAYS_IN_YEAR) if ser.std() > 0 else np.inf * ser.mean()

def resample(ser, how='mean', by=None):
    """Helper function.

    :param str how: One of the following: ('mean', 'ir', 'sr', 'count'). Default: 'mean'

    """

    if how == 'ir':
        return IR(ser) if by is None else ser.resample(by, how=IR)
    elif how == 'sr':
        return Sharpe(ser) if by is None else ser.resample(by, how=Sharpe)
    elif how == 'count':
        return ser.count() if by is None else ser.resample(by, how=lambda x: x.count())
    else:
        return ser.mean() if by is None else ser.resample(by, how='mean')
