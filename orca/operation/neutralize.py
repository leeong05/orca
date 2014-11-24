"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
from pandas.tseries.index import DatetimeIndex

from orca.mongo.industry import IndustryFetcher
from orca.utils import dateutil

from base import OperationBase


class GroupNeutOperation(OperationBase):
    """Class to neutralize alpha within a group.

    :param group: Groupings, either a Series(static grouping) or a DataFrame(dynamic grouping); in latter case, you are **advised** to make the index of type DatatimeIndex. Default: None
    """

    def __init__(self, group=None, **kwargs):
        super(GroupNeutOperation, self).__init__(**kwargs)
        self.group = group

    def operate(self, alpha):
        if self.group is None:
            return alpha.subtract(alpha.mean(axis=1), axis=0)

        res = {}
        for _, row in alpha.iterrows():
            date = row.name
            res[date] = self._operate(row, self.group if isinstance(self.group, pd.Series) else self.group.ix[date])
        return pd.DataFrame(res).T

    def _operate(self, alpha, group):
        sids = group[group.notnull()].index.intersection(alpha[alpha.notnull()].index)

        nalpha, group = alpha[sids], group[sids]
        nalpha = nalpha.groupby(group).transform(lambda x: x-x.mean())
        return nalpha.reindex(index=alpha.index)


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
            window = dateutil.to_datestr(alpha.index)
        else:
            window = list(alpha.index)
        group = self.industry.fetch_window(group, window)
        self.group = group
        return super(IndustryNeutOperation, self).operate(alpha)
