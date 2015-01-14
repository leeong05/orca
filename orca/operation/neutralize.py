"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import multiprocessing

import numpy as np
import pandas as pd

from orca.mongo.industry import IndustryFetcher
from orca.utils import dateutil

from base import OperationBase


def worker(args):
    dt, alpha, group = args
    sids = group.dropna().index
    nalpha, group = alpha[sids], group[sids]
    nalpha = nalpha.groupby(group).transform(lambda x: x-x.mean())
    return dt, nalpha

class GroupNeutOperation(OperationBase):
    """Class to neutralize alpha within a group.

    :param group: Groupings, either a Series(static grouping) or a DataFrame(dynamic grouping); in latter case, you are **advised** to make the index of type DatatimeIndex. Default: None
    """

    def __init__(self, group=None, threads=multiprocessing.cpu_count(), **kwargs):
        super(GroupNeutOperation, self).__init__(**kwargs)
        self.group = group
        self.threads = threads

    def operate(self, alpha, date=None):
        if isinstance(alpha, pd.Series):
            if self.group is None:
                return alpha - alpha.mean()
            if isinstance(self.group, pd.DataFrame):
                group = self.group.ix[date]
            else:
                group = self.group
            sids = group.dropna().index
            nalpha = alpha.ix[sids]
            nalpha = nalpha.groupby(group).transform(lambda x: x-x.mean())
            return nalpha.reindex(index=alpha.index)

        if self.group is None:
            return alpha.subtract(alpha.mean(axis=1), axis=0)
        if isinstance(self.group, pd.Series):
            sids = self.group.dropna().index
            nalpha = alpha.T.ix[sids]
            nalpha = nalpha.groupby(self.group.dropna()).transform(lambda x: x-x.mean()).T
            return nalpha.reindex(columns=alpha.columns)

        dates = dateutil.to_datestr(alpha.index)
        pool = multiprocessing.Pool(self.threads)
        res = pool.imap_unordered(worker, [(dt1, row, self.group.ix[dt2]) for (dt1, row), dt2 in zip(alpha.iterrows(), dates)])
        pool.close()
        pool.join()

        df = {}
        for dt, row in res:
            df[dt] = row
        return pd.DataFrame(df).T.reindex(columns=alpha.columns)


class IndustryNeutOperation(GroupNeutOperation):
    """Class to neutralize alpha by industry classifications.

    :param str standard: Industry classification standard, currently only supports: ('SW2014', 'ZX'). Default: 'SW2014'
    """

    def __init__(self, standard='SW2014', **kwargs):
        super(IndustryNeutOperation, self).__init__(**kwargs)
        self.standard = standard
        self.industry = IndustryFetcher(datetime_index=True)
        self.group = None

    def operate(self, alpha, group='sector', simple=False, date=None):
        if isinstance(alpha, pd.Series):
            group = self.industry.fetch_daily(group, date).dropna()
            nalpha = alpha.ix[group.index]
            nalpha = nalpha.groupby(group).transform(lambda x: x-x.mean())
            return nalpha.reindex(index=alpha.index)

        window = np.unique(dateutil.to_datestr(alpha.index))
        group = self.industry.fetch_window(group, window)
        self.group = group.iloc[-1] if simple else group
        return super(IndustryNeutOperation, self).operate(alpha)
