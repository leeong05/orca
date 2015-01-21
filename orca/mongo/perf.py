"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import DB

from base import KDayFetcher


class PerfFetcher(KDayFetcher):
    """Class to fetch performance metrics of alphas."""

    def __init__(self, **kwargs):
        self.collection = DB.performance
        super(PerfFetcher, self).__init__(**kwargs)

    def fetch(self, *args, **kwargs):
        return super(PerfFetcher, self).fetch(*args, **kwargs)

    def fetch_window(self, metric, window, alpha=None, mode='longshort', **kwargs):
        """
        :param alpha: Alpha name or a list of alpha names or None. Default: None, fetch all alphas
        :type alpha: str, list, None
        """
        datetime_index = kwargs.get('datetime_index', self.datetime_index)

        query = {'mode': mode, 'date': {'$lte': window[-1], '$gte': window[0]}}
        if alpha is not None:
            query['alpha'] = {'$in': [alpha] if isinstance(alpha, str) else alpha}
        proj = {'_id': 0, 'alpha': 1, 'date': 1, metric: 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame([(row['date'], row['alpha'], row[metric]) for row in cursor])
        df.columns = ['date', 'alpha', metric]
        df = df.pivot('date', 'alpha', metric)
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        if isinstance(alpha, str):
            return df[alpha]
        return df

    def fetch_history(self, *args, **kwargs):
        return super(PerfFetcher, self).fetch_history(*args, **kwargs)

    def fetch_daily(self, *args, **kwargs):
        return super(PerfFetcher, self).fetch_daily(*args, **kwargs)
