"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from db import alphadb


class AlphaDBFetcher(object):
    """Base class to fetch data from the alpha DB.

    :param str table: Table name

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, table):
        self.table = table


class ScoreFetcher(AlphaDBFetcher):
    """Class to fetch scores of an alpha from the alpha DB."""

    def __init__(self):
        super(ScoreFetcher, self).__init__('score')

    def fetch_id(



class UniverseFetcher(AlphaDBFetcher):

    pass

class Performance(AlphaDBFetcher):

    pass
