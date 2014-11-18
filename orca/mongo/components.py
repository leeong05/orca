"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DB
from base import KDayFetcher


class ComponentsFetcher(KDayFetcher):
    """Class to fetch index components data.

    :param boolean as_bool: Whether the returned result be a weight matrix or just a boolean matrix. Default: True

    """

    index_dname = {
            'HS300': 'SH000300',
            'CS500': 'SH000905',
            'CS800': 'SH000906',
            'SH50':  'SH000016',
            }

    dnames = DB.index_components.distinct('dname')

    def __init__(self, as_bool=True, **kwargs):
        self.collection = DB.index_components
        self.as_bool = as_bool
        super(ComponentsFetcher, self).__init__(**kwargs)
        self.delay = kwargs.get('delay', 0)

    def fetch_window(self, dname, *args, **kwargs):
        dname = self.index_dname.get(dname, dname)
        as_bool = kwargs.pop('as_bool', self.as_bool)
        df = super(ComponentsFetcher, self).fetch_window(dname, *args, **kwargs)
        if as_bool:
            return ~df.isnull()
        return df
