"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import (
        DATES,
        SIDS,
        )

def random_alpha(n=None):
    """Generate a random alpha(i.e. a DataFrame of random floats with DatetimeINdex and full sids columns).

    :param int n: Length of the returned DataFrame; when None, it will be a random number between 50 and 100. Default: None

    """
    if not n >= 0:
        n = np.random.randint(50, 100)
    dates = pd.to_datetime(DATES[2000:2000+n])
    df = pd.DataFrame(np.random.randn(n, len(SIDS)), index=dates, columns=SIDS)
    return df
