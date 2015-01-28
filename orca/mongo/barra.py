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

    industry_factors = {
            'daily': ['CNE5D_COUNTRY', 'CNE5D_ENERGY', 'CNE5D_CHEM', 'CNE5D_CONMAT', 'CNE5D_MTLMIN',
                      'CNE5D_MATERIAL', 'CNE5D_AERODEF', 'CNE5D_BLDPROD', 'CNE5D_CNSTENG', 'CNE5D_ELECEQP',
                      'CNE5D_INDCONG', 'CNE5D_MACH', 'CNE5D_TRDDIST', 'CNE5D_COMSERV', 'CNE5D_AIRLINE',
                      'CNE5D_MARINE', 'CNE5D_RDRLTRAN', 'CNE5D_AUTO', 'CNE5D_HOUSEDUR', 'CNE5D_LEISLUX',
                      'CNE5D_CONSSERV', 'CNE5D_MEDIA', 'CNE5D_RETAIL', 'CNE5D_PERSPRD', 'CNE5D_BEV',
                      'CNE5D_FOODPROD', 'CNE5D_HEALTH', 'CNE5D_BANKS', 'CNE5D_DVFININS', 'CNE5D_REALEST',
                      'CNE5D_SOFTWARE', 'CNE5D_HDWRSEMI', 'CNE5D_UTILITIE',
                      ],
            'short': ['CNE5S_COUNTRY', 'CNE5S_ENERGY', 'CNE5S_CHEM', 'CNE5S_CONMAT', 'CNE5S_MTLMIN',
                      'CNE5S_MATERIAL', 'CNE5S_AERODEF', 'CNE5S_BLDPROD', 'CNE5S_CNSTENG', 'CNE5S_ELECEQP',
                      'CNE5S_INDCONG', 'CNE5S_MACH', 'CNE5S_TRDDIST', 'CNE5S_COMSERV', 'CNE5S_AIRLINE',
                      'CNE5S_MARINE', 'CNE5S_RDRLTRAN', 'CNE5S_AUTO', 'CNE5S_HOUSEDUR', 'CNE5S_LEISLUX',
                      'CNE5S_CONSSERV', 'CNE5S_MEDIA', 'CNE5S_RETAIL', 'CNE5S_PERSPRD', 'CNE5S_BEV',
                      'CNE5S_FOODPROD', 'CNE5S_HEALTH', 'CNE5S_BANKS', 'CNE5S_DVFININS', 'CNE5S_REALEST',
                      'CNE5S_SOFTWARE', 'CNE5S_HDWRSEMI', 'CNE5S_UTILITIE',
                      ]
            }
    style_factors = {
            'daily': ['CNE5D_SIZE', 'CNE5D_BETA', 'CNE5D_MOMENTUM', 'CNE5D_RESVOL', 'CNE5D_SIZENL',
                      'CNE5D_BTOP', 'CNE5D_LIQUIDTY', 'CNE5D_EARNYILD', 'CNE5D_GROWTH', 'CNE5D_LEVERAGE',
                      ],
            'short': ['CNE5S_SIZE', 'CNE5S_BETA', 'CNE5S_MOMENTUM', 'CNE5S_RESVOL', 'CNE5S_SIZENL',
                      'CNE5S_BTOP', 'CNE5S_LIQUIDTY', 'CNE5S_EARNYILD', 'CNE5S_GROWTH', 'CNE5S_LEVERAGE',
                      ],
            }
    all_factors = {
            'daily': industry_factors['daily'] + style_factors['daily'],
            'short': industry_factors['short'] + style_factors['short'],
            }

    models = ('daily', 'short')

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
        dates = cls.idmaps.distinct('date')
        if date is None:
            date = dates[-1]
        else:
            date = dateutil.parse_date(dates, dateutil.compliment_datestring(date, -1, True), -1)

        query = {'date': str(date)}
        proj = {'_id': 0, 'idmaps': 1}
        dct = cls.idmaps.find_one(query, proj)['idmaps']
        if barra_key:
            return dct
        return {v: k for k, v in dct.iteritems()}

BarraFetcher.idmaps = DB.barra_idmaps


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
        self.dnames = self.all_factors[model]

    @property
    def factors(self):
        """Property with no setter."""
        return self.all_factors[self.model]

    def fetch_daily(self, *args, **kwargs):
        """This differs from the default :py:meth:`orca.mongo.base.KDayFetcher.fetch_daily` in only
        one aspect: when the ``dname`` is not given, this will fetch all factors exposure on ``date``.

        Also, you can provide one of ('industry', 'style') to fetch exposures to industry/style factors.

        :returns: Series(if a factor name is given), DataFrame(factor names are in the columns)
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

        if factor is not None and factor not in ('industry', 'style'):
            return super(BarraExposureFetcher, self).fetch_daily(*args, **kwargs)

        di, date = dateutil.parse_date(DATES, date, -1)
        date = DATES[di-offset]
        reindex = kwargs.get('reindex', self.reindex)

        query = {'date': date}
        proj = {'_id': 0, 'dname': 1, 'dvalue': 1}
        cursor = self.collection.find(query, proj)
        df = pd.DataFrame({row['dname']: row['dvalue'] for row in cursor})
        del cursor

        if reindex:
            df = df.reindex(index=SIDS)

        if factor == 'industry':
            return df[BarraFetcher.industry_factors[self.model]]
        elif factor == 'style':
            return df[BarraFetcher.style_factors[self.model]]
        return df


class BarraFactorFetcher(BarraFetcher):
    """Class to fetch factor returns/covariance data."""

    collections = {
        'daily': (DB.barra_D_returns, DB.barra_D_covariance, DB.barra_D_precovariance),
        'short': (DB.barra_S_returns, DB.barra_S_covariance, DB.barra_S_precovariance),
        }

    def __init__(self, model, **kwargs):
        super(BarraFactorFetcher, self).__init__(model, **kwargs)
        self.ret, self.cov, self.precov = BarraFactorFetcher.collections[model]

    @property
    def factors(self):
        """Property with no setter."""
        return self.all_factors[self.model]

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
        self.ret, self.cov = BarraFactorFetcher.collections[model]

    def fetch_returns(self, factor, window, **kwargs):
        """Fetch returns for factors.

        :param factor: Factor name or a list of factor names or one of ('industry', 'style'). Default: None, all factors will be fetched
        :type factor: None, str, list
        :returns: Series(if ``type(factor)`` is ``str``) or DataFrame
        """
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        if factor is None:
            factor = self.factors
        elif factor == 'industry':
            factor = self.industry_factors[self.model]
        elif factor == 'style':
            factor = self.style_factors[self.model]

        query = {'factor': {'$in': [factor] if isinstance(factor, str) else factor},
                'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'factor': 1, 'returns': 1, 'date': 1}
        cursor = self.ret.find(query, proj)
        df = pd.Series({(row['date'], row['factor']): row['returns'] for row in cursor}).unstack()
        del cursor
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df[factor] if isinstance(factor, str) else df

    def fetch_daily_covariance(self, date, prevra=False):
        """Fecth factor covariance matrix on a given date."""
        query = {'date': date}
        proj = {'_id': 0, 'factor': 1, 'covariance': 1}
        if prevra:
            cursor = self.precov.find(query, proj)
        else:
            cursor = self.cov.find(query, proj)
        df = pd.DataFrame({row['factor']: row['covariance'] for row in cursor})
        del cursor
        return df

    def fetch_covariance(self, factor, startdate=None, enddate=None, **kwargs):
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        prevra = kwargs.get('prevra', False)
        query = {'factor': factor,
                'date': {
                    '$lte': enddate if enddate else DATES[-1],
                    '$gte': startdate if startdate else DATES[0],
                    },
                }
        proj = {'_id': 0, 'date': 1, 'covariance': 1}
        if prevra:
            cursor = self.precov.find(query, proj)
        else:
            cursor = self.cov.find(query, proj)
        df = pd.DataFrame({row['date']: row['covariance'] for row in cursor}).T
        del cursor
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df

    def fetch_variance(self, factor, *args, **kwargs):
        return self.fetch_covariance(factor)[factor]

    def fetch(self, *args, **kwargs):
        """Use this method **only** if one wants to fetch returns."""
        try:
            dateutil.compliment_datestring(str(args[0]), -1, True)
            return super(BarraFactorFetcher, self).fetch(None, *args, **kwargs)
        except ValueError:
            return super(BarraFactorFetcher, self).fetch(*args, **kwargs)

    def fetch_window(self, *args, **kwargs):
        """Wrapper for :py:meth:`fetch_returns`.

        :param str dname: 'returns' or factor name
        """
        if isinstance(args[0], list):
            try:
                dateutil.compliment_datestring(args[0][0], -1, True)
                return self.fetch_returns(None, *args, **kwargs)
            except ValueError:
                pass
        return self.fetch_returns(*args, **kwargs)

    def fetch_history(self, *args, **kwargs):
        """Use this method **only** if one wants to fetch returns."""
        try:
            dateutil.compliment_datestring(str(args[0]), -1, True)
            return super(BarraFactorFetcher, self).fetch_history(None, *args, **kwargs)
        except ValueError:
            return super(BarraFactorFetcher, self).fetch_history(*args, **kwargs)

    def fetch_daily(self, dname, date, offset=0, **kwargs):
        """Wrapper for :py:meth:`fetch_returns` and :py:meth:`fetch_covariance`.

        :param str dname: 'returns', 'covariance'
        """
        date_check = kwargs.get('date_check', self.date_check)
        date = dateutil.compliment_datestring(date, -1, date_check)
        di, date = dateutil.parse_date(DATES, date, -1)
        date = DATES[di-offset]

        if dname == 'covariance':
            return self.fetch_covariance(date)

        factor = kwargs.get('factor', None)
        return self.fetch_returns(factor, [date], **kwargs).iloc[0]


class BarraCovarianceFetcher(BarraFetcher):
    """Class to fetch covariance data."""

    def __init__(self, model, **kwargs):
        if kwargs.get('reindex', False):
            kwargs['reindex'] = False
            self.warning('Force self.reindex to be False')
        if kwargs.get('datetime_index', False):
            kwargs['datetime_index'] = False
            self.warning('Force self.datetime_index to be False')
        super(BarraCovarianceFetcher, self).__init__(model, **kwargs)
        self.fexp = BarraExposureFetcher(model, **kwargs)
        self.fcov = BarraFactorFetcher(model, **kwargs)
        self.specifics = BarraSpecificsFetcher(model, **kwargs)
        self.suppress_warning = False

    @property
    def model(self):
        """Property."""
        return self._model

    @model.setter
    def model(self, model):
        self.fexp.model = model
        self.fcov.model = model
        self.specifics.model = model

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

    def fetch_daily(self, date, offset=0, sids=[], **kwargs):
        """Fetch the covariance matrix for a given set of stocks.

        :param list sids: The given set of stock ids
        """
        suppress_warning = kwargs.get('suppress_warning', self.suppress_warning)

        if sids is None or len(sids) == 0:
            sids = SIDS

        exposure = self.fexp.fetch_daily(date, offset=offset).ix[sids]
        exposure = exposure.dropna(axis=0, how='all').fillna(0)
        specific_risk = self.specifics.fetch_daily('specific_risk', date, offset=offset).ix[sids].dropna()

        nsids = specific_risk.index.intersection(exposure.index)
        exposure, specific_risk = exposure.ix[nsids], specific_risk.ix[nsids]

        if len(nsids) != len(sids) and not suppress_warning:
            self.warning('Some sids may not be in Barra universe and will be dropped from the result')
        factor_cov = self.fcov.fetch_daily('covariance', date, offset=offset)
        return exposure.dot(factor_cov).dot(exposure.T) + pd.DataFrame(np.diag(specific_risk ** 2), index=nsids, columns=nsids)
