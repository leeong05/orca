"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DATES

from base import KDayFetcher
from quote import QuoteFetcher
from kday import CaxFetcher
import util

BACKWARD, FORWARD = 1, -1


class AdjQuoteFetcher(KDayFetcher):
    """Class to fetch adjusted price/volume data.

    :param int mode: Mode to adjust price/volume. Default: BACKWARD(1)

    """

    _prices = ('prev_close', 'close', 'open', 'high', 'low', 'vwap')
    _volumes = ('volume')
    _noadjust = ('returns', 'amount')

    def __init__(self, mode=BACKWARD, **kwargs):
        self.mode = mode
        self.quote = QuoteFetcher(**kwargs)
        self.cax = CaxFetcher(**kwargs)
        super(AdjQuoteFetcher, self).__init__(**kwargs)

    def _adjust_price(price, adj_factor, date):
        adj_price = adj_factor.ix[price.index] * price
        adj_price = adj_price.div(adj_factor.ix[date], axis=1)
        return adj_price

    @staticmethod
    def _adjust_volume(volume, adj_factor, date):
        adj_factor = 1. / adj_factor
        adj_volume = adj_factor.ix[volume.index] * volume
        adj_volume = adj_volume.div(adj_factor.ix[date], axis=1)
        return adj_volume

    def _get_adjust_window(self, window, basedate=None, **kwargs):
        date_check = kwargs.get('date_check', self.date_check)

        if basedate is None:
            basedate = window[0] if self.mode == FORWARD else window[-1]
            adj_window = window
        else:
            sdi, startdate = util.parse_date(DATES, window[0])
            edi, enddate = util.parse_date(DATES, window[-1])
            basedate = util.compliment_datestring(str(basedate), self.mode, date_check)
            bdi, basedate = util.parse_date(DATES, basedate, self.mode)
            if self.mode == FORWARD:
                if bdi > sdi:
                    raise ValueError('With FORWARD mode, basedate %s cannot be larger than startdate %s' % (repr(basedate), repr(startdate)))
                adj_window = DATES[bdi: edi+1]
            else:
                if bdi < edi:
                    raise ValueError('With BACKWARD mode, basedate %s cannot be smaller than enddate %s' % (repr(basedate), repr(startdate)))
                adj_window = DATES[sdi: bdi+1]
        return adj_window

    def fetch(self, dname, startdate, enddate=None, backdays=0, basedate=None, **kwargs):
        """
        :param basedate: Date on which the data adjusting is based
        :type basedate: str or None. Default: None, defaults to ``enddate``

        """
        dname = dname[4:]
        if dname in self._noadjust:
            return self.quote.fetch(dname, startdate, enddate)

        date_check = kwargs.get('date_check', self.date_check)
        window = util.cut_window(
                DATES,
                util.compliment_datestring(str(startdate), -1, date_check),
                util.compliment_datestring(str(enddate), -1, date_check) if enddate is not None else None,
                backdays=backdays)
        return self.fetch_window(dname, window, basedate, **kwargs)

    def fetch_window(self, dname, window, basedate, **kwargs):
        """
        :param str basedate: Date on which the data adjusting is based

        """
        adj_window = self._get_adjust_window(window, basedate, **kwargs)
        df = self.quote.fetch_window(dname, window, **kwargs)
        if dname in self._prices:
            adj = self.cax.fetch_window('adjfactor', adj_window)
            return self._adjust_price(df, adj, basedate)
        elif dname in self._volumes:
            adj = self.cax.fetch_window('volfactor', adj_window)
            return self._adjust_volume(df, adj, basedate)
        else:
            raise ValueError('%s is not a valid data name' % repr(dname))

    def fetch_history(self, dname, date, backdays, **kwargs):
        """This will always use ``BACKWARD`` mode with ``basedate`` equal to ``date``."""
        dname = dname[4:]
        if dname in self._noadjust:
            return self.quote.fetch_history(dname, date, backdays, **kwargs)

        original_mode = self.mode
        self.mode = BACKWARD

        date_check = kwargs.get('date_check', self.date_check)
        delay = kwargs.get('date_check', self.delay)
        date = util.compliment_datestring(str(date), -1, date_check)
        di, date = util.parse_date(DATES, date, -1)
        di -= delay
        window = DATES[di-backdays+1: di+1]
        df = self.fetch_window(dname, window, date, **kwargs)

        self.mode = original_mode
        return df

    def fetch_daily(self, *args, **kwargs):
        raise NotImplementedError('Non-sensical to fetch just one day adjusted data')
