"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import multiprocessing

import numpy as np
import pandas as pd

from orca.mongo.barra import (
        BarraSpecificsFetcher,
        BarraExposureFetcher,
        BarraFactorFetcher,
        BarraCovarianceFetcher,
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


def worker1(args):
    obj, alpha, factor, date, dt = args
    try:
        date = date.strftime('%Y%m%d')
    except:
        pass

    if isinstance(factor, str):
        if factor == 'industry':
            factor = obj.industry_factors
        elif factor == 'style':
            factor = obj.style_factors
        else:
            factor = obj.all_factors

    finite_alpha = alpha[np.isfinite(alpha)]
    exposure = obj.exposure.fetch_daily(date, offset=1)[factor]
    exposure = exposure.dropna(axis=0, how='all').fillna(0)
    sids = exposure.index.intersection(finite_alpha.index)

    nalpha, exposure = alpha[sids], exposure.ix[sids]
    a, b = exposure.T.dot(exposure), exposure.T.dot(nalpha)
    try:
        lamb = np.linalg.solve(a, b)
    except np.linalg.linalg.LinAlgError:
        obj.warning('Singular matrix, neutralization failed')
        return dt, alpha
    nalpha = pd.Series(nalpha.values - exposure.dot(lamb), index=sids)
    return dt, nalpha.reindex(index=alpha.index)


class BarraFactorNeutOperation(BarraOperation):
    """Class to neutralize alpha along some Barra factors."""

    def __init__(self, model, threads=multiprocessing.cpu_count(), **kwargs):
        super(BarraFactorNeutOperation, self).__init__(model, **kwargs)
        self.threads = threads

#    def _operate(self, alpha, factor, date):
#        """
#        :param Series alpha: Row extracted from an alpha DataFrame
#        :param factor: Factors to be neutralized. When it is a string, it must take value in ('industry', 'style', 'all')
#        :type factor: str, list
#        """
#        try:
#            date = date.strftime('%Y%m%d')
#        except:
#            pass
#
#        if isinstance(factor, str):
#            if factor == 'industry':
#                factor = self.industry_factors
#            elif factor == 'style':
#                factor = self.style_factors
#            else:
#                factor = self.all_factors
#
#        finite_alpha = alpha.dropna()
#
#        exposure = self.exposure.fetch_daily(date, offset=1)[factor]
#        exposure = exposure.dropna(axis=0, how='all').fillna(0)
#        sids = exposure.index.intersection(finite_alpha.index)
#
#        nalpha, exposure = alpha[sids], exposure.ix[sids]
#        a, b = exposure.T.dot(exposure), exposure.T.dot(nalpha)
#        try:
#            lamb = np.linalg.solve(a, b)
#        except np.linalg.linalg.LinAlgError:
#            self.warning('Singular matrix, neutralization failed')
#            return alpha
#        nalpha = pd.Series(nalpha.values - exposure.dot(lamb), index=sids)
#        return nalpha.reindex(index=alpha.index)

    def operate(self, alpha, factor):
        """
        :param factor: Factors to be neutralized. When it is a string, it must take value in ('industry', 'style', 'all')
        :type factor: str, list
        """
        pool = multiprocessing.Pool(self.threads)
        res = pool.imap_unordered(worker1, [(self, row, factor, date, date) for date, row in alpha.iterrows()])
        pool.close()
        pool.join()

        df = {}
        for dt, row in res:
            df[dt] = row
        return pd.DataFrame(df).T


def worker2(args):
    obj, alpha, factor, date, dt = args
    try:
        date = date.strftime('%Y%m%d')
    except:
        pass

    if isinstance(factor, str):
        if factor == 'industry':
            factor = obj.industry_factors
        elif factor == 'style':
            factor = obj.style_factors
        else:
            factor = obj.all_factors

    finite_alpha = alpha[np.isfinite(alpha)]

    exposure = obj.exposure.fetch_daily(date, offset=1)[factor]
    exposure = exposure.dropna(axis=0, how='all').fillna(0)
    sids = exposure.index.intersection(finite_alpha.index)

    covariance = obj.covariance.fetch_daily(date, offset=1, sids=sids)
    sids = sids.intersection(covariance.index)

    nalpha, exposure, covariance = alpha[sids], exposure.ix[sids], covariance.ix[sids, sids]

    a, b = exposure.T.dot(covariance).dot(exposure), exposure.T.dot(covariance).dot(nalpha)
    try:
        lamb = np.linalg.solve(a, b)
    except np.linalg.linalg.LinAlgError:
        obj.warning('Singular matrix, neutralization failed')
        return dt, alpha
    nalpha = pd.Series(nalpha.values - exposure.dot(lamb), index=sids)
    return dt, nalpha.reindex(index=alpha.index)


class BarraFactorCorrNeutOperation(BarraOperation):
    """Class to neutralize alpha along some Barra factors."""

    def __init__(self, model, **kwargs):
        super(BarraFactorCorrNeutOperation, self).__init__(model, **kwargs)
        self.covariance = BarraCovarianceFetcher(model, **self.kwargs)

    def _operate(self, alpha, factor, date):
        """
        :param Series alpha: Row extracted from an alpha DataFrame
        :param factor: Factors to be neutralized. When it is a string, it must take value in ('industry', 'style', 'all')
        :type factor: str, list
        """
        try:
            date = date.strftime('%Y%m%d')
        except:
            pass

        if isinstance(factor, str):
            if factor == 'industry':
                factor = self.industry_factors
            elif factor == 'style':
                factor = self.style_factors
            else:
                factor = self.all_factors

        finite_alpha = alpha.dropna()

        exposure = self.exposure.fetch_daily(date, offset=1)[factor]
        exposure = exposure.dropna(axis=0, how='all').fillna(0)
        sids = exposure.index.intersection(finite_alpha.index)

        covariance = self.covariance.fetch_daily(date, offset=1, sids=sids)
        sids = sids.intersection(covariance.index)

        nalpha, exposure, covariance = alpha[sids], exposure.ix[sids], covariance.ix[sids, sids]

        a, b = exposure.T.dot(covariance).dot(exposure), exposure.T.dot(covariance).dot(nalpha)
        try:
            lamb = np.linalg.solve(a, b)
        except np.linalg.linalg.LinAlgError:
            self.warning('Singular matrix, neutralization failed')
            return alpha
        nalpha = pd.Series(nalpha.values - exposure.dot(lamb), index=sids)
        return nalpha.reindex(index=alpha.index)

    def operate(self, alpha, factor):
        """
        :param factor: Factors to be neutralized. When it is a string, it must take value in ('industry', 'style', 'all')
        :type factors: str, list
        """
        pool = multiprocessing.Pool(self.threads)
        res = pool.imap_unordered(worker2, [(self, row, factor, date, date) for date, row in alpha.iterrows()])
        pool.close()
        pool.join()

        df = {}
        for dt, row in res:
            df[dt] = row
        return pd.DataFrame(df).T
