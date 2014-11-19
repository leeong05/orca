"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.quote import QuoteFetcher
from orca.mongo.kday import SharesFetcher

from base import (
        SimpleDataFilter,
        DataFilter,
        )

def create_quote_filter(clsname, dname):
    """Factory function to create :py:class:`orca.universe.base.SimpleDataFilter` subclasses based on quote data."""
    def init(self, *args, **kwargs):
        SimpleDataFilter.__init__(self, (dname, QuoteFetcher), *args, **kwargs)
    return type(clsname, (SimpleDataFilter,), {'__init__': init})

def multiply(df1, df2):
    return df1 * df2

def divide(df1, df2):
    return df1 / df2

def create_multiply_data_filter(clsname, data1, data2):
    """Factory function to create :py:class:`orca.universe.base.DataFilter` subclasses with synthetic function as multiplying.

    :param tuple data1, data2: A 2/3-tuple like ``(dname, fetcherclass[, kwargs])``

    """
    def init(self, *args, **kwargs):
        DataFilter.__init__(self, [data1, data2], multiply, *args, **kwargs)
    return type(clsname, (DataFilter,), {'__init__': init})

def create_divide_data_filter(clsname, data1, data2):
    """Factory function to create :py:class:`orca.universe.base.DataFilter` subclasses with synthetic function as multiplying.

    :param tuple data1, data2: A 2/3-tuple like ``(dname, fetcherclass[, kwargs])``

    """
    def init(self, *args, **kwargs):
        DataFilter.__init__(self, [data1, data2], divide, *args, **kwargs)
    return type(clsname, (DataFilter,), {'__init__': init})

def create_cap_filter(clsname, shares):
    """Special case of :py:func:`create_multiply_data_filter` with one of the data being ``close``.

    :param str shares: Shares definition, must be one of the following 6: ('a_shares', 'a_float', 'a_float_listed', 'a_float_nonrestricted', 'a_float_restricted', 'float_shares'). Default: 'a_float_nonrestricted')

    """
    def init(self, *args, **kwargs):
        DataFilter.__init__(self, [('close', QuoteFetcher), (shares, SharesFetcher)], multiply, *args, **kwargs)
    return type(clsname, (DataFilter,), {'__init__': init})
