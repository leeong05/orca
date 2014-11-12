"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np

from base import OperationBase


class DecayOperation(OperationBase):
    """Class to linearly combine current alpha with decayed alphas, usually to reduce turnover.

    :param int days: How many days of decayed alphas to be included in final result
    :param boolean dense: Whether to treat ``NaN`` as 0 in current alpha. Default: False
    """

    def __init__(self, days, dense=False, **kwargs):
        super(DecayOperation, self).__init__(**kwargs)
        self.days = days
        self.dense = dense

    def operate(self, alpha):
        current = (alpha.fillna(0) if self.dense else alpha) * self.days
        for i in range(1, self.days):
            current += alpha.shift(i).fillna(0) * (self.days - i)
        current[alpha.fillna(method='ffill', limit=self.days-1).isnull()] = np.nan
        return current
