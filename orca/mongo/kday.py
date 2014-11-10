"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DB

from base import KDayFetcher


class CaxFetcher(KDayFetcher):
    """Class to fetch adjusting factors data."""

    def __init__(self, **kwargs):
        self.collection = DB.cax
        super(CaxFetcher, self).__init__(**kwargs)

    def fetch_window(self, *args, **kwargs):
        df = super(CaxFetcher, self).fetch_window(*args, **kwargs)
        return df.fillna(method='ffill').fillna(1)


class SharesFetcher(KDayFetcher):
    """Class to fetch shares structure data."""

    def __init__(self, **kwargs):
        self.collection = DB.shares
        super(SharesFetcher, self).__init__(**kwargs)


class ZYYXConsensusFetcher(KDayFetcher):
    """Class to fetch ZYYX analyst consensus data."""

    def __init__(self, **kwargs):
        self.collection = DB.zyconsensus
        super(ZYYXConsensusFetcher, self).__init__(**kwargs)
