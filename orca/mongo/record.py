"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DB

from base import RecordFetcher


class ZYYXScoreAdjustFetcher(RecordFetcher):
    """Class to fetch ZYYX score adjust records."""

    def __init__(self, **kwargs):
        self.collection = DB.zyscore_adjust
        super(ZYYXScoreAdjustFetcher, self).__init__(**kwargs)


class ZYYXReportAdjustFetcher(RecordFetcher):
    """Class to fetch ZYYX report adjust records."""

    def __init__(self, **kwargs):
        self.collection = DB.zyreport_adjust
        super(ZYYXReportAdjustFetcher, self).__init__(**kwargs)


class JYFundRecordFetcher(RecordFetcher):
    """Class to fetch collections 'jyis', 'jybs', 'jycs'."""

    collections = {
            'balancesheet': DB.jybs,
            'income': DB.jyis,
            'cashflow': DB.jycs,
            }

    def __init__(self, **kwargs):
        super(JYFundRecordFetcher, self).__init__(**kwargs)

    def fetch_window(self, *args, **kwargs):
        self.collection = JYFundRecordFetcher.collections[kwargs.pop('table')]
        return super(JYFundRecordFetcher, self).fetch_window(*args, **kwargs)


class JYDataRecordFetcher(RecordFetcher):
    """Class to fetch collection 'jydata'."""

    def __init__(self, **kwargs):
        super(JYDataRecordFetcher, self).__init__(**kwargs)
        self.collection = DB.jydata


class JYIndexRecordFetcher(RecordFetcher):
    """Class to fetch collection 'jyindex'."""

    def __init__(self, **kwargs):
        super(JYIndexRecordFetcher, self).__init__(**kwargs)
        self.collection = DB.jyindex
