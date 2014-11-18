"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import DB

from base import KDayFetcher
from industry import IndustryFetcher


class SYWGQuoteFetcher(KDayFetcher):
    """Class to fetch SYWG index quote data.

    :param int level: Which industry level index data is of interest? Default: 1
    :param boolean use_industry: Returned DataFrame use industry code as columns? Default: True
    """

    dnames = ['open', 'high', 'low', 'close', 'prevclose', 'volume', 'amount', 'returns']

    def __init__(self, level=1, use_industry=True, **kwargs):
        self.collection = DB.sywgindex_quote
        self.level = level
        self.use_industry = use_industry
        if self.use_industry:
            self.fetcher = IndustryFetcher()
        super(SYWGQuoteFetcher, self).__init__(**kwargs)

    def fetch_window(self, dname, window, **kwargs):
        level = kwargs.get('level', self.level)
        datetime_index = kwargs.get('datetime_index', self.datetime_index)

        query = {'dname': dname, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'dvalue': 1, 'date': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
        if self.use_industry:
            industry_index = self.fetcher.fetch_info('index', level, date=window[-1])
            index_industry = {v: k for k, v in industry_index.iteritems()}

            res = {}
            for index in df.columns:
                if index in index_industry:
                    res[index_industry[index]] = df[index]
            df = pd.DataFrame(res)
        return self.format(df, datetime_index, False)
