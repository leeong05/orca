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

    _industry_factors = ['ENERGY', 'CHEM', 'CONMAT', 'MTLMIN',
                      'MATERIAL', 'AERODEF', 'BLDPROD', 'CNSTENG', 'ELECEQP',
                      'INDCONG', 'MACH', 'TRDDIST', 'COMSERV', 'AIRLINE',
                      'MARINE', 'RDRLTRAN', 'AUTO', 'HOUSEDUR', 'LEISLUX',
                      'CONSSERV', 'MEDIA', 'RETAIL', 'PERSPRD', 'BEV',
                      'FOODPROD', 'HEALTH', 'BANKS', 'DVFININS', 'REALEST',
                      'SOFTWARE', 'HDWRSEMI', 'UTILITIE',
                      ]
    industry_factors = {
            'daily': ['CNE5D_'+factor for factor in _industry_factors],
            'short': ['CNE5S_'+factor for factor in _industry_factors],
            }
    _style_factors = ['SIZE', 'BETA', 'MOMENTUM', 'RESVOL', 'SIZENL',
                      'BTOP', 'LIQUIDTY', 'EARNYILD', 'GROWTH', 'LEVERAGE',
                      ]
    style_factors = {
            'daily': ['CNE5D_'+factor for factor in _style_factors],
            'short': ['CNE5S_'+factor for factor in _style_factors],
            }
    _all_factors = _industry_factors+_style_factors
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
            date = dateutil.parse_date(dates, dateutil.compliment_datestring(date, -1, True), -1)[1]

        query = {'date': str(date)}
        proj = {'_id': 0, 'idmaps': 1}
        dct = cls.idmaps.find_one(query, proj)['idmaps']
        if barra_key:
            return dct
        inv_dct = {}
        for k, v in dct.iteritems():
            if v not in inv_dct or inv_dct[v] > k:
                inv_dct[v] = k
        return inv_dct

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
    prefixes = {
            'daily': 'CNE5D',
            'short': 'CNE5S',
            }

    def __init__(self, model, **kwargs):
        super(BarraFactorFetcher, self).__init__(model, **kwargs)
        self.ret, self.cov, self.precov = BarraFactorFetcher.collections[model]
        self.prefix = BarraFactorFetcher.prefixes[model]

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
        elif isinstance(factor, str):
            factor = factor.find('_') == -1 and self.prefix+'_'+factor or factor
            assert factor in self.all_factors[self.model]
        else:
            factor = [f.find('_') == -1 and self.prefix+'_'+f or f for f in factor]
            assert all([f in self.all_factors[self.model] for f in factor])

        query = {'factor': {'$in': [factor] if isinstance(factor, str) else factor},
                'date': {'$gte': window[0], '$lte': window[-1]}}
        proj = {'_id': 0, 'factor': 1, 'returns': 1, 'date': 1}
        cursor = self.ret.find(query, proj)
        df = pd.Series({(row['date'], row['factor']): row['returns'] for row in cursor}).unstack()
        del cursor
        if datetime_index:
            df.index = pd.to_datetime(df.index)
        return df[factor] if isinstance(factor, str) else df

    def fetch_daily_covariance(self, date, **kwargs):
        """Fecth factor covariance matrix on a given date."""
        return self.fetch_covariance(startdate=date, enddate=date, **kwargs)[date]

    def fetch_covariance(self, factor=None, startdate=None, enddate=None, **kwargs):
        if isinstance(factor, str):
            factor = factor.find('_') == -1 and self.prefix+'_'+factor or factor
            assert factor in self.all_factors[self.model]
        datetime_index = kwargs.get('datetime_index', self.datetime_index)
        prevra = kwargs.get('prevra', False)
        date_check = kwargs.get('date_check', self.date_check)
        if enddate:
            enddate = dateutil.compliment_datestring(str(enddate), 1, date_check)
            enddate = dateutil.parse_date(DATES, enddate, -1)[1]
        else:
            enddate = DATES[-1]
        if startdate:
            startdate = dateutil.compliment_datestring(str(startdate), -1, date_check)
            startdate = dateutil.parse_date(DATES, startdate, 1)[1]
        else:
            startdate = DATES[0]
        query = {'date': {'$lte': enddate, '$gte': startdate}}
        if isinstance(factor, str):
            query['factor'] = factor
        proj = {'_id': 0, 'date': 1, 'covariance': 1, 'factor': 1}
        if prevra:
            cursor = self.precov.find(query, proj)
        else:
            cursor = self.cov.find(query, proj)
        if factor:
            res = pd.DataFrame({row['date']: row['covariance'] for row in cursor}).T
            if datetime_index:
                res.index = pd.to_datetime(res.index)
        else:
            res = pd.DataFrame({(row['date'], row['factor']): row['covariance'] for row in cursor}).T
            res = pd.Panel({date: res.ix[date] for date in res.unstack().index})
            if datetime_index:
                res.items = pd.to_datetime(res.items)
        del cursor
        return res

    def fetch_variance(self, factor, *args, **kwargs):
        return self.fetch_covariance(factor, *args, **kwargs)[factor]

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
            return self.fetch_daily_covariance(date)

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

    def fetch_daily(self, date, offset=0, sids=[], factor=None, both=False, **kwargs):
        """Fetch the covariance matrix for a given set of stocks.

        :param list sids: The given set of stock ids
        """
        suppress_warning = kwargs.get('suppress_warning', self.suppress_warning)

        sids = sids or SIDS

        exposure = self.fexp.fetch_daily(date, offset=offset).ix[sids]
        exposure = exposure.dropna(axis=0, how='all').fillna(0)
        specific_risk = self.specifics.fetch_daily('specific_risk', date, offset=offset).ix[sids].dropna()

        nsids = specific_risk.index.intersection(exposure.index)
        exposure, specific_risk = exposure.ix[nsids], specific_risk.ix[nsids]

        if len(nsids) != len(sids) and not suppress_warning:
            self.warning('Some sids may not be in Barra universe and will be dropped from the result')
        factor_cov = self.fcov.fetch_daily('covariance', date, offset=offset)
        if both:
            return exposure.dot(factor_cov).dot(exposure.T), pd.DataFrame(np.diag(specific_risk ** 2), index=nsids, columns=nsids)
        if factor is True:
            return exposure.dot(factor_cov).dot(exposure.T)
        elif factor is False:
            return pd.DataFrame(np.diag(specific_risk ** 2), index=nsids, columns=nsids)
        else:
            return exposure.dot(factor_cov).dot(exposure.T) + pd.DataFrame(np.diag(specific_risk ** 2), index=nsids, columns=nsids)
