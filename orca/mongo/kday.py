"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import DB

from base import KDayFetcher


class CaxFetcher(KDayFetcher):
    """Class to fetch adjusting factors data."""

    dnames = DB.cax.distinct('dname')

    def __init__(self, **kwargs):
        self.collection = DB.cax
        super(CaxFetcher, self).__init__(**kwargs)

    def fetch_window(self, *args, **kwargs):
        """This will fill all missing value as 1."""
        df = super(CaxFetcher, self).fetch_window(*args, **kwargs)
        return df.fillna(method='ffill').fillna(1)


class SharesFetcher(KDayFetcher):
    """Class to fetch shares structure data."""

    dnames = DB.shares.distinct('dname')

    def __init__(self, **kwargs):
        self.collection = DB.shares
        super(SharesFetcher, self).__init__(**kwargs)


class ZYYXConsensusFetcher(KDayFetcher):
    """Class to fetch ZYYX analyst consensus data."""

    dnames = DB.zyconsensus.distinct('dname')

    def __init__(self, **kwargs):
        self.collection = DB.zyconsensus
        super(ZYYXConsensusFetcher, self).__init__(**kwargs)


class EODValueFetcher(KDayFetcher):
    """Class to fetch data from collection 'eod_value'."""

    dnames = DB.eod_value.distinct('dname')

    def __init__(self, **kwargs):
        self.collection = DB.eod_value
        super(EODValueFetcher, self).__init__(**kwargs)


class MiscFetcher(KDayFetcher):
    """Class to fetch tradable and other miscellaneous data."""

    dnames = DB.misc.distinct('dname')

    def __init__(self, **kwargs):
        self.collection = DB.misc
        super(MiscFetcher, self).__init__(**kwargs)

    def fetch_daily(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KDayFetcher.fetch_daily`.

        One can provide a boolean keyword argument ``as_list`` to return the valid sids."""
        as_list = kwargs.get('as_list', False)
        ser = super(MiscFetcher, self).fetch_daily(*args, **kwargs)
        if as_list:
            return list(ser.dropna().index)


class UnivFetcher(KDayFetcher):
    """Class to fetch some common universes."""

    dnames = DB.universe.distinct('dname')

    def __init__(self, **kwargs):
        self.collection = DB.universe
        super(UnivFetcher, self).__init__(**kwargs)

    def fetch_window(self, *args, **kwargs):
        res = super(UnivFetcher, self).fetch_window(*args, **kwargs)
        return res.notnull()

    def fetch_daily(self, *args, **kwargs):
        """A variant of :py:meth:`orca.mongo.base.KDayFetcher.fetch_daily`.

        One can provide a boolean keyword argument ``as_list`` to return the valid sids."""
        as_list = kwargs.get('as_list', False)
        ser = super(UnivFetcher, self).fetch_daily(*args, **kwargs)
        if as_list:
            return list(ser[ser].index)


class AlphaFetcher(KDayFetcher):
    """Class to fetch raw alpha data."""

    dnames = DB.alpha.distinct('dname')

    def __init__(self, **kwargs):
        self.collection = DB.alpha
        super(AlphaFetcher, self).__init__(**kwargs)
