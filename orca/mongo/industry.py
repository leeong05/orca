"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import (
        DB,
        DATES,
        )
from orca.utils import dateutil

from base import KDayFetcher


class IndustryFetcher(KDayFetcher):
    """Class to fetch industry data.

    :param str standard: Industry classification standard, currently only supports: ('SW2014', 'ZX'). Default: 'SW2014'
    """

    name_dname = {
            'sector': 'level1',
            'industry': 'level2',
            'subindustry': 'level3',
            }

    def __init__(self, standard='SW2014', **kwargs):
        self.standard = standard
        self.collection = DB.industry
        self.info = DB.industry_info
        super(IndustryFetcher, self).__init__(**kwargs)

    def fetch_window(self, dname, window, **kwargs):
        """By supplying ``standard`` as a keyword argument, one can override the default setting."""
        dname = self.name_dname.get(dname, dname)
        standard = kwargs.get('standard', self.standard)
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        reindex = kwargs.get('reindex', self.reindex)

        query = {'standard': standard, 'dname': dname, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'dvalue': 1, 'date': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
        del cursor
        return self.format(df, datetime_index, reindex)

    def fetch_info(self, dname='name', level=0, date=None, **kwargs):
        """Fetch industry-name/industry-index correspondance.

        :param str dname: 'name'(default): fetch industry-name mapping; 'index': fetch industry-index mapping
        :param int level: Which level of industry is of interest? Default: 0, all 3 levels' information are fetched
        :rtype: dict
        """
        standard = kwargs.get('standard', self.standard)
        date_check = kwargs.get('date_check', self.date_check)

        if date is not None:
            date = dateutil.parse_date(DATES, dateutil.compliment_datestring(date, -1, date_check), -1)[1]
        else:
            date = self.info.distinct('date')[-1]

        query = {'standard': standard, 'date': date}
        if level == 0:
            query.update({'dname': 'industry_%s' % dname})
        else:
            query.update({'dname': 'level%d_%s' % (level, dname)})
        proj = {'_id': 0, 'dvalue': 1}

        return self.info.find_one(query, proj)['dvalue']
