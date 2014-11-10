"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import (
        DATES,
        SIDS,
        )
from orca.mongo.quote import QuoteFetcher
from orca.mongo.components import ComponentsFetcher
from orca.mongo.industry import IndustryFetcher
from orca.mongo import util as mongo_util

from base import (
        FilterBase,
        SimpleDataFilter,
        )
import rules


class TickerFilter(FilterBase):
    """Class to filter out stocks by ticker patterns. Usually, stock ticker
    pattern contains information about its listed market.

    :param function rule: String manipulation function

    """

    def __init__(self, rule=None, **kwargs):
        self.rule = rule
        self.series = pd.Series({sid: self.rule(sid) for sid in SIDS})
        super(TickerFilter, self).__init__(**kwargs)

    def filter(self, startdate, enddate=None, parent=None, **kwargs):
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        reindex = kwargs.get('reindex', self.reindex)
        date_check = kwargs.get('date_check', self.date_check)

        univ_window = mongo_util.cut_window(
                DATES,
                mongo_util.compliment_datestring(str(startdate), -1, date_check),
                mongo_util.compliment_datestring(str(enddate), 1, date_check) if enddate is not None else None)
        index, columns = univ_window, SIDS
        df = pd.DataFrame(0, index, columns)
        df = df.add(self.series.astype(int), axis=1)

        self.comply(df, parent, False)
        return self.format(df, datetime_index, reindex)


class TradingDaysFilter(SimpleDataFilter):
    """Class to filter out stocks by their active trading history. In particular,
    this can filter new IPO stocks.

    :param function rule: Rule like: :py:func:`orca.universe.rules.count_gt`

    """

    def __init__(self, window, rule=None, **kwargs):
        super(TradingDaysFilter, self).__init__(
                ('close', QuoteFetcher),
                window,
                rule=rule,
                **kwargs)


class ActiveFilter(SimpleDataFilter):
    """Class to filter out stocks when they are suspended."""

    def __init__(self, **kwargs):
        super(ActiveFilter, self).__init__(
                ('close', QuoteFetcher),
                1,
                rule=rules.is_finite(),
                delay=0,
                **kwargs)


class ComponentsFilter(SimpleDataFilter):
    """Class to filter out stocks that are components in an index.

    :param str index: Index name, currently only supports: 'HS300', 'CS500', 'CS800', 'SH50'
    """

    def __init__(self, index, **kwargs):
        SimpleDataFilter.__init__(
                self,
                (index, ComponentsFetcher),
                1,
                rule=rules.identity(),
                delay=0,
                **kwargs)


class IndustryFilter(SimpleDataFilter):
    """Class to filter out stocks based on their industry classification.

    :param list industry: Indutries that should be included in the universe
    :param str standard: Industry classification standard, currently only supports: 'SW2014', 'SW', 'ZX'. Default: 'SW2014'
    """

    def __init__(self, industry, standard='SW2014', **kwargs):
        SimpleDataFilter.__init__(
                self,
                ('sector', IndustryFetcher, {'standard': standard}),
                1,
                rule=rules.isin(industry),
                delay=0,
                **kwargs)
