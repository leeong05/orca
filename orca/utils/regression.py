"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

import statsmodels.api as sm


def get_beta(y, x, add_intercept=True, half_life=None):
    x = pd.DataFrame(x)
    y = y.ix[np.isfinite(y)]
    x = x.ix[y.index]
    if add_intercept:
        x = sm.add_constant(x)
    if isinstance(half_life, int):
        w = pd.Series({date: 0.5**(i*1./half_life) for i, date in enumerate(reversed(x.index))})
    else:
        w = pd.Series({date: 1 for date in x.index})
    return sm.WLS(y, x, weights=1./w).fit().params[1]

def get_slope(y, add_intercept=True, half_life=None):
    x = pd.Series(range(len(y)), index=y.index)
    return get_beta(y, x, add_intercept=add_intercept, half_life=half_life)
