"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import multiprocessing

import numpy as np
import pandas as pd

from orca import DATES

from orca.mongo.barra import (
        BarraSpecificsFetcher,
        BarraExposureFetcher,
        BarraFactorFetcher,
        )

from base import OperationBase

class BarraOperation(OperationBase):
    """Base class for operations based on Barra model data.

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
        if model not in BarraOperation.models:
            raise ValueError('No such version {0!r} of Barra model exists'.format(model))
        self._model = model
        self.kwargs = kwargs
        super(BarraOperation, self).__init__(**kwargs)

        self.industry_factors = BarraFactorNeutOperation.industry_factors[model]
        self.style_factors = BarraFactorNeutOperation.style_factors[model]
        self.all_factors = BarraFactorNeutOperation.all_factors[model]

        self.specifics = BarraSpecificsFetcher(model, **self.kwargs)
        self.exposure = BarraExposureFetcher(model, **self.kwargs)
        self.factor = BarraFactorFetcher(model, **self.kwargs)

    @property
    def model(self):
        return self._model

    @model.setter
    def model(self, model):
        if model not in BarraOperation.models:
            self.warning('No such version {0!r} of Barra model exists. Nothing has changed'.format(model))
            return
        self._model = model
        self.specifics = BarraSpecificsFetcher(model, **self.kwargs)
        self.exposure = BarraExposureFetcher(model, **self.kwargs)
        self.factor = BarraFactorFetcher(model, **self.kwargs)

    def parse_factors(self, factors):
        if isinstance(factors, str):
            factors = [factors]
        elif not factors:
            factors = self.all_factors
        assert isinstance(factors, list)
        _factors = set()
        for factor in factors:
            if factor == 'industry':
                _factors = _factors | set(self.industry_factors)
            elif factors == 'style':
                _factors = _factors | set(self.style_factors)
            if factor in self.all_factors:
                _factors = set(self.all_factors)
        return list(_factors)


def worker1(args):
    date, alpha, exposure = args

    finite_alpha = alpha.dropna()
    exposure = exposure.dropna(axis=0, how='all').fillna(0)
    sids = exposure.index.intersection(finite_alpha.index)

    nalpha, exposure = alpha[sids], exposure.ix[sids]
    a, b = exposure.T.dot(exposure), exposure.T.dot(nalpha)
    try:
        lamb = np.linalg.solve(a, b)
    except np.linalg.linalg.LinAlgError:
        return date, alpha, False
    nalpha = pd.Series(nalpha.values - exposure.dot(lamb), index=sids)
    return date, nalpha.reindex(index=alpha.index), True


class BarraFactorNeutOperation(BarraOperation):
    """Class to neutralize alpha along some Barra factors."""

    def __init__(self, model, threads=multiprocessing.cpu_count(), **kwargs):
        super(BarraFactorNeutOperation, self).__init__(model, **kwargs)
        self.threads = threads

    def operate(self, alpha, factors):
        """
        :param factors: Factors to be neutralized. When it is a string, it must take value in ('industry', 'style', 'all')
        :type factors: str, list
        """
        factors = self.parse_factors(factors)

        if isinstance(alpha.index, pd.tseries.index.DatetimeIndex):
            datetime_index = True
            last_date = alpha.index[-1].strftime('%Y%m%d')
        else:
            datetime_index = False
            last_date = alpha.index[-1]

        exposures = {}
        for factor in factors:
            exposure = self.exposure.fetch_history(
                    factor,
                    last_date,
                    len(alpha),
                    delay=1,
                    datetime_index=datetime_index)
            exposure.index = alpha.index
            exposures[factor] = exposure
        exposures = pd.Panel(exposures)
        alpha = alpha[np.isfinite(alpha)]

        pool = multiprocessing.Pool(self.threads)
        res = pool.imap_unordered(
                worker1,
                ((date, alpha.ix[date], exposures.major_xs(date)) for date in alpha.index)
                )
        pool.close()
        pool.join()

        df = {}
        for date, alpha, sucess in res:
            if not sucess:
                self.warning('Failed to neutralize on {}'.format(alpha.name))
            df[date] = alpha
        df = pd.DataFrame(df).T
        return df


def worker2(args):
    factors, date, alpha, exposure, fcov, spec = args

    finite_alpha = alpha.dropna()
    exposure = exposure.dropna(axis=0, how='all').fillna(0)
    spec = spec.dropna()
    sids = exposure.index.intersection(finite_alpha.index).intersection(spec.index)

    nalpha, exposure, spec = alpha[sids], exposure.ix[sids], spec.ix[sids]
    covariance = exposure.dot(fcov).dot(exposure.T) + pd.DataFrame(np.diag(spec ** 2), index=sids, columns=sids).fillna(0)

    exposure = exposure[factors]
    a, b = exposure.T.dot(covariance).dot(exposure), exposure.T.dot(covariance).dot(nalpha)
    try:
        lamb = np.linalg.solve(a, b)
    except np.linalg.linalg.LinAlgError:
        return date, alpha, False
    nalpha = pd.Series(nalpha.values - exposure.dot(lamb), index=sids)
    return date, nalpha.reindex(index=alpha.index), True


class BarraFactorCorrNeutOperation(BarraOperation):
    """Class to neutralize alpha along some Barra factors."""

    def __init__(self, model, threads=multiprocessing.cpu_count(), **kwargs):
        super(BarraFactorCorrNeutOperation, self).__init__(model, **kwargs)
        self.threads = threads

    def operate(self, alpha, factors):
        """
        :param factors: Factors to be neutralized. When it is a string, it must take value in ('industry', 'style', 'all')
        :type factors: str, list
        """
        factors = self.parse_factors(factors)

        if isinstance(alpha.index, pd.tseries.index.DatetimeIndex):
            datetime_index = True
            last_date = alpha.index[-1].strftime('%Y%m%d')
        else:
            datetime_index = False
            last_date = alpha.index[-1]

        exposures = {}
        for factor in self.all_factors:
            exposure = self.exposure.fetch_history(
                    factor,
                    last_date,
                    len(alpha),
                    delay=1,
                    datetime_index=datetime_index)
            exposure.index = alpha.index
            exposures[factor] = exposure
        exposures = pd.Panel(exposures)

        startdate, enddate = DATES[DATES.index(last_date)-len(alpha)], DATES[DATES.index(last_date)-1]
        covariances = self.factor.fetch_covariance(
                startdate=startdate,
                enddate=enddate,
                datetime_index=datetime_index)
        covariances.items = alpha.index
        df = covariances[last_date]

        specifics = self.specifics.fetch_history(
                'specific_risk', last_date, len(alpha), delay=1, datetime_index=datetime_index)
        specifics.index = alpha.index

        alpha = alpha[np.isfinite(alpha)]

        pool = multiprocessing.Pool(self.threads)
        res = pool.imap_unordered(
                worker2,
                ((factors, date, alpha.ix[date], exposures.major_xs(date), covariances[date], specifics.ix[date]) for date in alpha.index)
                )
        pool.close()
        pool.join()

        df = {}
        for date, alpha, sucess in res:
            if not sucess:
                self.warning('Failed to neutralize on {}'.format(alpha.name))
            df[date] = alpha
        df = pd.DataFrame(df).T
        return df
