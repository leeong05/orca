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
