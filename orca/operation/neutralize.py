"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from datetime import datetime
import multiprocessing

import numpy as np
import pandas as pd
from pandas.tseries.index import DatetimeIndex

from orca.mongo.industry import IndustryFetcher
from orca.utils import dateutil

from base import OperationBase


def worker(args):
    dt, alpha, group = args
    if isinstance(dt, datetime):
        date = dt.strftime('%Y%m%d')
    else:
        date = dt
    if not isinstance(group, pd.Series):
        group = group.ix[date]
    sids = group[group.notnull()].index.intersection(alpha[alpha.notnull()].index)
    nalpha, group = alpha[sids], group[sids]
    nalpha = nalpha.groupby(group).transform(lambda x: x-x.mean())
    return dt, nalpha.reindex(index=alpha.index)

class GroupNeutOperation(OperationBase):
    """Class to neutralize alpha within a group.

    :param group: Groupings, either a Series(static grouping) or a DataFrame(dynamic grouping); in latter case, you are **advised** to make the index of type DatatimeIndex. Default: None
    """

    def __init__(self, group=None, threads=multiprocessing.cpu_count(), **kwargs):
        super(GroupNeutOperation, self).__init__(**kwargs)
        self.group = group
        self.threads = threads

    def operate(self, alpha):
        if self.group is None:
            return alpha.subtract(alpha.mean(axis=1), axis=0)

        pool = multiprocessing.Pool(self.threads)
        res = pool.imap_unordered(worker, [(dt, row, self.group) for dt, row in alpha.iterrows()])
        pool.close()
        pool.join()

        df = {}
        for dt, row in res:
            df[dt] = row
        return pd.DataFrame(df).T


class IndustryNeutOperation(GroupNeutOperation):
    """Class to neutralize alpha by industry classifications.

    :param str standard: Industry classification standard, currently only supports: ('SW2014', 'ZX'). Default: 'SW2014'
    """

    def __init__(self, standard='SW2014', **kwargs):
        super(IndustryNeutOperation, self).__init__(**kwargs)
        self.standard = standard
        self.industry = IndustryFetcher(datetime_index=True)
        self.group = None

    def operate(self, alpha, group='sector'):
        if isinstance(alpha.index, DatetimeIndex):
            window = np.unique(dateutil.to_datestr(alpha.index))
        else:
            window = list(alpha.index)
        group = self.industry.fetch_window(group, window)
        self.group = group
        return super(IndustryNeutOperation, self).operate(alpha)
