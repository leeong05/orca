"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import DAYS_IN_YEAR

def drawdown(ser):
    ser = ser.cumsum()
    end = (pd.expanding_max(ser)-ser).argmax()
    start = ser.ix[:end].argmax()

    return start, end, ser[start]-ser[end]

def annualized_returns(ser):
    return ser.mean() * DAYS_IN_YEAR / ser.count()

def perwin(ser):
    return (ser > 0).sum() * 100. / ser.count()

def IR(ser):
    return ser.mean() / ser.std()

def Sharpe(ser):
    return ser.mean() / ser.std() * np.sqrt(DAYS_IN_YEAR)

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
