"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import (
        DATES,
        SIDS,
        )
from orca.utils import dateutil

from base import FilterBase


class ChainFilter(FilterBase):
    """Class to filter out a universe chainly.

    :param list filters: List of filters to go through; each item is either a filter itself or a tuple of filters. Each filterd universe serves as the parent universe for remainders

    .. note::

       The keyword parameter ``reindex`` is always coerced to be True.
    """

    def __init__(self, filters, **kwargs):
        super(ChainFilter, self).__init__(**kwargs)
        self._filters = filters
        if self.reindex is not True:
            self.warning('Force self.reindex to be True')
            self.reindex = True

    def filter(self, startdate, enddate=None, parent=None, **kwargs):
        if 'datetime_index' not in kwargs:
            kwargs.update({'datetime_index': self.datetime_index})
        kwargs.update({'reindex': self.reindex})
        if 'date_check' not in kwargs:
            kwargs.update({'date_check': self.date_check})
        date_check = kwargs.get('date_check', self.date_check)

        univ_window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None)
        startdate, enddate = univ_window[0], univ_window[-1]
        # at first, the universe is full, i.e. all sids are included
        parent = pd.DataFrame(True,
                index=pd.to_datetime(univ_window) if self.datetime_index else univ_window,
                columns=SIDS)
        for elem in self._filters:
            # if the item is a tuple of filter, we first call their filter method and then intersect the results
            if isinstance(elem, tuple) or isinstance(elem, list):
                res = [f.filter(startdate, enddate, parent=parent, **kwargs) for f in elem]
                for df in res:
                    parent = parent & df
            # simply call its filter method
            else:
                parent = elem.filter(startdate, enddate, parent=parent, **kwargs)
        return parent


class NegateFilter(FilterBase):
    """Class to negate a filter.

    :param filter: Filter to be negated
    """

    def __init__(self, filter, **kwargs):
        super(NegateFilter, self).__init__(**kwargs)
        self._filter = filter

    def filter(self, startdate, enddate=None, parent=None, **kwargs):
        kwargs.update({'return_parent': True})
        parent, df = self._filter.filter(startdate, enddate=enddate, parent=parent, **kwargs)
        if parent is None:
            return ~df
        return ~df & parent


class UnionFilter(FilterBase):
    """Class to union the universes filtered by two seperate filters.

    :param filters: List of filters to be unioned. Each item must be a filter itself

    .. note::

       The keyword parameter ``reindex`` is always coerced to be True.
    """

    def __init__(self, filters, **kwargs):
        super(UnionFilter, self).__init__(**kwargs)
        self._filters = filters
        if self.reindex is not True:
            self.warning('Force self.reindex to be True')
            self.reindex = True

    def filter(self, startdate, enddate=None, parent=None, **kwargs):
        if 'datetime_index' not in kwargs:
            kwargs.update({'datetime_index': self.datetime_index})
        kwargs.update({'reindex': self.reindex})
        if 'date_check' not in kwargs:
            kwargs.update({'date_check': self.date_check})
        date_check = kwargs.get('date_check', self.date_check)

        univ_window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None)
        startdate, enddate = univ_window[0], univ_window[-1]
        res = [elem.filter(startdate, enddate=enddate, parent=parent, **kwargs) for elem in self._filters]
        seed = res.pop()
        for df in res:
            seed = seed | df
        return seed
