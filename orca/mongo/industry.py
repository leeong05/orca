"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import (
        DB,
        DATES,
        )

from base import KDayFetcher
import util


class IndustryFetcher(KDayFetcher):
    """Class to fetch industry data.

    :param str standard: Industry classification standard, currently only supports: 'SW2014', 'SW', 'ZX'. Default: 'SW2014'

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
        dname = self.name_dname.get(dname, dname)
        standard = kwargs.get('standard', self.standard)
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        reindex = kwargs.get('reindex', self.reindex)

        query = {'standard': standard, 'dname': dname, 'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'dvalue': 1, 'date': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['date']: row['dvalue'] for row in cursor}).T
        return self.format(df, datetime_index, reindex)

    def fetch_info(self, level, date=None, standard='SW2014', **kwargs):
        """Fetch industry-name corespondance.

        :param int level: Which level of industry is of interest? Default: 0, all 3 levels' information are fetched

        """
        standard = kwargs.get('standard', self.standard)
        date_check = kwargs.get('date_check', self.date_check)

        if date is not None:
            date = util.parse_date(DATES, util.compliment_datestring(date, -1, date_check), -1)[1]
        else:
            date = DATES[-1]

        query = {'standard': standard, 'date': date}
        if level == 0:
            query.update({'dname': 'industry_name'})
        else:
            query.update({'dname': 'level%d_name' % level})
        proj = {'_id': 0, 'dvalue': 1}

        return self.info.find_one(query, proj)['dvalue']
