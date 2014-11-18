"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import DB

from base import KDayFetcher


class IndexQuoteFetcher(KDayFetcher):
    """Class to fetch index quote data."""

    index_dname = {
            'HS300': 'SH000300',
            'CS500': 'SH000905',
            'CS800': 'SH000906',
            }
    dnames = ['amount', 'close', 'high', 'low', 'open', 'prev_close', 'returns', 'volume', 'vwap']

    def __init__(self, **kwargs):
        self.collection = DB.index_quote
        super(IndexQuoteFetcher, self).__init__(**kwargs)
        if self.reindex is True:
            self.warning('Force reindex to be False')
            self.reindex = False

    def fetch_window(self, dname, window, index=None, **kwargs):
        """
        :param dname: Data name or a list of data names
        :type dname: str, list
        :param str index: Index name
        :returns: Series if ``dname`` is only a string or DataFrame with ``dname`` in the columns

        """
        assert index is not None
        index = IndexQuoteFetcher.index_dname.get(index, index)
        datetime_index = kwargs.get('datetime_index', self.datetime_index)

        query = {'index': index, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0}
        _dname = [dname] if isinstance(dname, str) else dname
        for d in _dname:
            proj.update({d: 1})
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame(list(cursor))
        df.index = pd.to_datetime(df.date) if datetime_index else df.date
        return df[dname]
