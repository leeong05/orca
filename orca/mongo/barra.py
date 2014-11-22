"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from orca import (
        DB,
        DATES,
        SIDS,
        )
from orca.utils import dateutil

from base import KDayFetcher

class BarraFetcher(KDayFetcher):
    """Base class for Barra model data fetchers.

    :param str model: Model version, currently only supports: ('daily', 'short')

    .. note::

       This is a base class and should not be used directly.
    """

    models = ('daily', 'short')

    idmaps = DB.barra_idmaps

    def __init__(self, model, **kwargs):
        if model not in BarraFetcher.models:
            raise ValueError('No such version {0!r} of Barra model exists'.format(model))
        self._model = model
        super(BarraFetcher, self).__init__(**kwargs)

    @property
    def model(self):
        """Property."""
        return self._model

    @model.setter
    def model(self, model):
        if model not in BarraFetcher.models:
            self.warning('No such version {0!r} of Barra model exists. Nothing has changed'.format(model))
            return
        self._model = model
        self.collection = self.__class__.collections[model]

    @classmethod
    def fetch_idmaps(cls, date=None, barra_key=True):
        """Fetch barra id - local stock id correspondance.

        :param boolean barra_key: Whether to use barra ids as keys. Default: True
        :returns: A ``dict``
        """
        if date is None:
            date = cls.idmaps.distinct('date')[-1]
        query = {'date': str(date)}
        proj = {'_id': 0, 'idmaps': 1}
        dct = cls.idmaps.find_one(query, proj)['idmaps']
        if barra_key:
            return dct
        return {v: k for k, v in dct.iteritems()}


class BarraSpecificsFetcher(BarraFetcher):
    """Class to fetch stock specifics in Barra model."""

    collections = {
        'daily': DB.barra_D_specifics,
        'short': DB.barra_S_specifics,
        }
    dnames = DB.barra_D_specifics.distinct('dname')

    def __init__(self, model, **kwargs):
        super(BarraSpecificsFetcher, self).__init__(model, **kwargs)
        self.collection = BarraSpecificsFetcher.collections[model]


class BarraExposureFetcher(BarraFetcher):
    """Class to fetch stock to factor exposure."""

    collections = {
        'daily': DB.barra_D_exposure,
        'short': DB.barra_S_exposure,
        }

    def __init__(self, model, **kwargs):
        super(BarraExposureFetcher, self).__init__(model, **kwargs)
        self.collection = BarraExposureFetcher.collections[model]
        self.dnames = self.collection.distinct('dname')

    def fetch_daily(self, *args, **kwargs):
        """This differs from the default :py:meth:`orca.mongo.base.KDayFetcher.fetch_daily` in only
        one aspect: when the ``dname`` is not given, this will fetch all factors exposure on ``date``.

        :returns: Series(if a factor name is given), DataFrame(all factor names are in the columns)
        """
        factor, date, offset = None, None, 0
        if 'offset' in kwargs:
            offset = int(kwargs.pop('offset'))
        # is the first argument a date?
        try:
            date = dateutil.compliment_datestring(str(args[0]), -1, True)
            # yes, it is a date
            if len(args) > 1:
                offset = int(args[1])
        except ValueError:
        # the first argument is not a date, presumably, it is the factor name!
            factor, date = args[0], args[1]
            # offset provided as the 3rd argument?
            if len(args) > 2:
                offset = int(args[2])

        if factor is not None:
            return super(BarraExposureFetcher, self).fetch_daily(*args, **kwargs)

        di, date = dateutil.parse_date(DATES, date, -1)
        date = DATES[di-offset]

        reindex = kwargs.get('reindex', self.reindex)
        query = {'date': date}
        proj = {'_id': 0, 'dname': 1, 'dvalue': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['dname']: row['dvalue'] for row in cursor})
        if reindex:
            return df.reindex(index=SIDS)
        return df


class BarraFactorFetcher(KDayFetcher):
    """Class to fetch factor returns/covariance data."""

    collections = {
        'daily': (DB.barra_D_returns, DB.barra_D_covariance),
        'short': (DB.barra_S_returns, DB.barra_S_covariance),
        }

    def __init__(self, model, **kwargs):
        super(BarraFactorFetcher, self).__init__(model, **kwargs)
        self.ret, self.cov = BarraFactorFetcher.collections[model]
        self._factors = self.ret.distinct('factor')

    @property
    def factors(self):
        """Property with no setter."""
        return self._factors

    @property
    def model(self):
        """Property."""
        return self._model

    @model.setter
    def model(self, model):
        if model not in BarraFetcher.models:
            self.warning('No such version {0!r} of Barra model exists. Nothing has changed'.format(model))
            return
        self._model = model
        self.ret, self.cov = self.__class__.collections[model]
        self._factors = self.ret.distinct('factor')

    def fetch_returns(self, factor, window, **kwargs):
        """Fetch returns for factors.

        :param factor: Factor name or a list of factor names. Default: None, all factors will be fetched
        :type factor: None, str, list
        :returns: Series(if ``type(factor)`` is ``str``) or DataFrame
        """
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        if factor is None :
            factor = self._factors

        query = {'factor': {'$in': [factor] if isinstance(factor, str) else factor},
                'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'factor': 1, 'returns': 1, 'date': 1}
        cursor = self.ret.find(query, proj)
        df = pd.Series({(row['date'], row['factor']): row['returns'] for row in cursor}).unstack()
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df[factor] if isinstance(factor, str) else df

    def fetch_covariance(self, date):
        """Fecth factor covariance matrix on a given date."""
        query = {'date': date}
        proj = {'_id': 0, 'factor': 1, 'covariance': 1}
        cursor = self.cov.find(query, proj)
        df = pd.DataFrame({row['factor']: row['covariance'] for row in cursor})
        return df

    def fetch(self, dname, *args, **kwargs):
        """
        :param str dname: 'returns' or factor name
        """
        if dname == 'returns':
            dname = None
        return super(BarraFactorFetcher, self).fetch(dname, *args, **kwargs)

    def fetch_window(self, dname, window, **kwargs):
        """Wrapper for :py:meth:`fetch_returns`.

        :param str dname: 'returns' or factor name
        """
        if dname == 'returns':
            dname = None
        return self.fetch_returns(dname, window, **kwargs)

    def fetch_history(self, dname, *args, **kwargs):
        """
        :param str dname: 'returns' or factor name

        """
        if dname == 'returns':
            dname = None
        return super(BarraFactorFetcher, self).fetch_history(dname, *args, **kwargs)

    def fetch_daily(self, dname, date, offset=0, **kwargs):
        """Wrapper for :py:meth:`fetch_returns` and :py:meth:`fetch_covariance`.

        :param str dname: 'returns', 'covariance' or any factor name
        """
        if dname == 'covariance':
            date_check = kwargs.get('date_check', self.date_check)
            date = dateutil.compliment_datestring(date, -1, date_check)
            di, date = dateutil.parse_date(DATES, date, -1)
            date = DATES[di-offset]
            return self.fetch_covariance(date)

        if dname == 'returns':
            dname = None
        return super(BarraFactorFetcher, self).fetch_daily(dname, date, offset=offset, **kwargs)


class BarraCovarianceFetcher(KDayFetcher):
    """Class to fetch factor returns/covariance data."""

    def __init__(self, model, **kwargs):
        if kwargs.get('reindex', False):
            kwargs['reindex'] = False
            self.warning('Force self.reindex to be False')
        if kwargs.get('datetime_index', False):
            kwargs['datetime_index'] = False
            self.warning('Force self.datetime_index to be False')
        super(BarraFactorFetcher, self).__init__(**kwargs)
        self.fexp = BarraExposureFetcher(model, **kwargs)
        self.fcov = BarraFactorFetcher(model, **kwargs)
        self.specifics = BarraSpecificsFetcher(model, **kwargs)

    def fetch(self, *args, **kwargs):
        """
        :raises: NotImplementedError
        """
        raise NotImplementedError

    def fetch_window(self, *args, **kwargs):
        """
        :raises: NotImplementedError
        """
        raise NotImplementedError

    def fetch_history(self, *args, **kwargs):
        """
        :raises: NotImplementedError
        """
        raise NotImplementedError

    def fetch_daly(self, date, offset=0, sids=[], **kwargs):
        """Fetch the covariance matrix for a given set of stocks.

        :param list sids: The given set of stock ids
        """
        exposure = self.fexp.fetch_daily(date, offset=offset).ix[sids]
        exposure = exposure.dropna(axis=0, how='all').fillna(0)
        specific_risk = self.specifics.fetch_daily(date, offset=offset).ix[sids].dropna()

        nsids = specific_risk.index.intersection(exposure.index)
        exposure, specific_risk = exposure.ix[nsids], specific_risk.ix[nsids]

        if len(nsids) != len(sids):
            self.warning('Some sids may not be in Barra universe and will be dropped from the result')
        factor_cov = self.fcov.fetch_daily('covariance', date, offset=offset)
        return exposure.dot(factor_cov).dot(exposure.T) + pd.DataFrame(np.diag(specific_risk ** 2), index=nsids, columns=nsids)
