"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DATES
from orca.utils import dateutil

from base import KDayFetcher
from quote import QuoteFetcher
from kday import CaxFetcher


class AdjQuoteFetcher(KDayFetcher):
    """Class to fetch adjusted price/volume data.

    :param int mode: Mode to adjust price/volume. Default: AdjQuoteFetcher.BACKWARD(1)
    """

    BACKWARD, FORWARD = 1, -1

    _prices = ('prev_close', 'close', 'open', 'high', 'low', 'vwap')
    _volumes = ('volume')
    _noadjust = ('returns', 'amount')

    dnames = ['adj_' + dname for dname in QuoteFetcher.dnames]

    def __init__(self, **kwargs):
        self.mode = kwargs.get('mode', AdjQuoteFetcher.BACKWARD)
        self.quote = QuoteFetcher(**kwargs)
        self.cax = CaxFetcher(**kwargs)
        super(AdjQuoteFetcher, self).__init__(**kwargs)

    @staticmethod
    def _adjust_price(price, adj_factor, date):
        adj_factor = adj_factor.ix[:, price.columns].fillna(1)
        adj_price = adj_factor.ix[price.index] * price
        adj_price = adj_price.div(adj_factor.ix[date], axis=1)
        return adj_price

    @staticmethod
    def _adjust_volume(volume, adj_factor, date):
        adj_factor = 1. / adj_factor
        adj_factor = adj_factor.ix[:, volume.columns].fillna(1)
        adj_volume = adj_factor.ix[volume.index] * volume
        adj_volume = adj_volume.div(adj_factor.ix[date], axis=1)
        return adj_volume

    def _get_adjust_window(self, window, basedate=None, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)

        if basedate is None:
            basedate = window[0] if self.mode == AdjQuoteFetcher.FORWARD else window[-1]
            adj_window = window
        else:
            sdi, startdate = dateutil.parse_date(DATES, window[0], 1)
            edi, enddate = dateutil.parse_date(DATES, window[-1], -1)
            basedate = dateutil.compliment_datestring(str(basedate), self.mode, date_check)
            bdi, basedate = dateutil.parse_date(DATES, basedate, self.mode)
            if self.mode == AdjQuoteFetcher.FORWARD:
                if bdi > sdi:
                    raise ValueError('With AdjQuoteFetcher.FORWARD mode, basedate %s cannot be larger than startdate %s' % (repr(basedate), repr(startdate)))
                adj_window = DATES[bdi: edi+1]
            else:
                if bdi < edi:
                    raise ValueError('With AdjQuoteFetcher.BACKWARD mode, basedate %s cannot be smaller than enddate %s' % (repr(basedate), repr(startdate)))
                adj_window = DATES[sdi: bdi+1]
        return adj_window

    def fetch(self, dname, startdate, enddate=None, backdays=None, basedate=None, **kwargs):
        """
        :param basedate: Date on which the data adjusting is based. Default: None, defaults to ``enddate``
        :type basedate: str, None
        """
        if dname[:4] in self._noadjust:
            return self.quote.fetch(dname, startdate, enddate, **kwargs)

        date_check = kwargs.get('date_check', self.date_check)
        window = dateutil.cut_window(
                DATES,
                dateutil.compliment_datestring(str(startdate), -1, date_check),
                dateutil.compliment_datestring(str(enddate), -1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, window, basedate, **kwargs)

    def fetch_window(self, dname, window, basedate=None, **kwargs):
        """
        :param str basedate: Date on which the data adjusting is based
        """
        dname = dname[4:]
        if dname in self._noadjust:
            return self.quote.fetch_window(dname, window, **kwargs)

        adj_window = self._get_adjust_window(window, basedate=basedate, **kwargs)
        if basedate is None:
            basedate = window[0] if self.mode == AdjQuoteFetcher.FORWARD else window[-1]

        df = self.quote.fetch_window(dname, window, **kwargs)
        if dname in self._prices:
            adj = self.cax.fetch_window('adjfactor', adj_window, **kwargs)
            return self._adjust_price(df, adj, basedate)
        elif dname in self._volumes:
            adj = self.cax.fetch_window('volfactor', adj_window, **kwargs)
            return self._adjust_volume(df, adj, basedate)
        else:
            raise ValueError('%s is not a valid data name' % repr(dname))

    def fetch_history(self, dname, date, backdays, **kwargs):
        """This will always use ``AdjQuoteFetcher.BACKWARD`` mode with ``basedate`` equal to ``date``."""
        if dname[:4] in self._noadjust:
            return self.quote.fetch_history(dname[:4], date, backdays, **kwargs)

        original_mode = self.mode
        self.mode = AdjQuoteFetcher.BACKWARD

        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('delay', self.delay)
        date = dateutil.compliment_datestring(str(date), -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        df = self.fetch_window(dname, window, date, **kwargs)

        self.mode = original_mode
        return df

    def fetch_daily(self, *args, **kwargs):
        """Non-sensical to fetch just one day adjusted data.

        :raises: NotImplementedError
        """
        raise NotImplementedError
