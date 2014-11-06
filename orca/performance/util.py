"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefunc.om>
"""

import numpy as np
import pandas as pd

from orca import DAYS_IN_YEAR

def drawdown(ser):
    """Calculate drawdown on a given returns series.

    :returns: start, end, drawdown
    :rtype: tuple
    ser = ser.cumsum()
    """

    ser = ser.cumsum()
    end = (pd.expanding_max(ser) - ser).argmax()
    start = ser.ix[:end].argmax()
    dd = ser[start] - ser[end]
    return (start, end, dd)

def annualized_returns(ser):
    """Calculate annualized returns."""

    return ser.mean() / len(ser) * DAYS_IN_YEAR * 100

def perwin(ser):
    """Calculate winning percentage."""

    return (ser > 0).sum() * 100. / len(ser)

def IR(ser):
    """Calculate IR."""

    return ser.mean() / ser.std() * np.sqrt(DAYS_IN_YEAR)
