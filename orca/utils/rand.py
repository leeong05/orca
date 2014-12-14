"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import (
        DATES,
        SIDS,
        )
from orca.utils import dateutil

def random_alpha(startdate='20140103', n=None):
    """Generate a random alpha(i.e. a DataFrame of random floats with DatetimeINdex and full sids columns).

    :param startdate: Starting point. Default: '20140103'
    :type startdate: int, str
    :param int n: Length of the returned DataFrame; when None, it will be a random number between 50 and 100. Default: None
    """
    if not n >= 0:
        n = np.random.randint(50, 100)
    dates = dateutil.get_startfrom(DATES,
                                   dateutil.compliment_datestring(str(startdate), -1, True),
                                   n)
    df = pd.DataFrame(np.random.randn(n, len(SIDS)), index=dates, columns=SIDS)
    return df

def rand_intalpha(startdate='20140103', freq='30min', n=None):
    if not n >= 0:
        n = np.random.randint(10, 20)
    dates = dateutil.get_startfrom(DATES,
                                   dateutil.compliment_datestring(str(startdate), -1, True),
                                   n)
    times = dateutil.generate_intervals(int(freq[:-3])*60)
    dts = pd.to_datetime([d+' '+t for d in dates for t in times])
    df = pd.DataFrame(np.random.randn(len(dts), len(SIDS)), index=dts, columns=SIDS)
    return df
