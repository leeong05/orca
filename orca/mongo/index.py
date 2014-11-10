"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DB

from base import KDayFetcher


class IndexQuoteFetcher(KDayFetcher):
    """Class to fetch index quote data."""

    index_dname = {
            'SHZS': 'SH000001',
            'AGZS': 'SH000002',
            'BGZS': 'SH000003',
            'ZHZS': 'SH000008',
            'SH380': 'SH000009',
            'SH180': 'SH000010',
            'SH150': 'SH000133',
            'SH100': 'SH000132',
            'SH50': 'SH000016',
            'JJZS': 'SH000011',
            'GZZS': 'SH000012',
            'SHXZZ': 'SH000017',
            'HS300': 'SH0003000',
            'SZCZ': 'SZ399001',
            'CFAZ': 'SZ399002',
            'CFBZ': 'SZ399003',
            'SZ100': 'SZ399004',
            'ZXBI': 'SZ399005',
            'CYBI': 'SZ399006',
            'SHXZS': 'SZ399100',
            'ZXBZ': 'SZ399101',
            'SZZZ': 'SZ399106',
            'SZAZ': 'SZ399107',
            'SZBZ': 'SZ399108',
            'ZXBR': 'SZ399333',
            'CYBR': 'SZ399606',
            'JCAZ': 'SZ399317',
            }

    def __init__(self, **kwargs):
        self.collection = DB.index_quote
        super(IndexQuoteFetcher, self).__init__(**kwargs)
        if self.reindex is True:
            self.warning('Force reindex to be False')
            self.reindex = False

    def fetch_window(self, *args, **kwargs):
        if kwargs.get('reindex', False) is True:
            kwargs['reindex'] = False
        super(IndexQuoteFetcher, self).fetch_window(*args, **kwargs)
