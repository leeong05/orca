"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

import statsmodels.api as sm
import sklearn.linear_model as skl

from base import AlphaCombinerBase


class OLSCombiner(AlphaCombinerBase):

    def __init__(self, periods, **kwargs):
        super(OLSCombiner, self).__init__(periods, **kwargs)

    def fit(self, X, Y):
        X, Y = X.ix[Y.notnull()], Y.ix[Y.notnull()]
        results = sm.OLS(Y, X).fit()
        self.info('Regression summary:\n{}'.format(results.summary()))
        X = self.data.iloc[:, 2:-1]
        return pd.Series(results.predict(X), index=X.index).unstack()

    def normalize(self):
        self.data[:, 2:-1] = self.data[:, 2:-1].fillna(0)


class QuantRegCombiner(AlphaCombinerBase):

    def __init__(self, periods, q, **kwargs):
        super(QuantRegCombiner, self).__init__(periods, **kwargs)
        self.q = q

    def fit(self, X, Y):
        X, Y = X.ix[Y.notnull()], Y.ix[Y.notnull()]
        results = sm.QuantReg(Y, X).fit(self.q)
        self.info('Regression summary:\n{}'.format(results.summary()))
        X = self.data.iloc[:, 2:-1]
        return pd.Series(results.predict(X), index=X.index).unstack()

    def normalize(self):
        self.data[:, 2:-1] = self.data[:, 2:-1].fillna(0)


class RidgeCombiner(AlphaCombinerBase):

    def __init__(self, periods, alpha, fit_intercept=False, **kwargs):
        super(RidgeCombiner, self).__init__(periods, **kwargs)
        self.alpha = alpha
        self.fit_intercept = fit_intercept

    def fit(self, X, Y):
        X, Y = X.ix[Y.notnull()], Y.ix[Y.notnull()]
        ridge = skl.Ridge(alpha=self.alpha, fit_intercept=self.fit_intercept)
        ridge.fit(X, Y)
        X = self.data.iloc[:, 2:-1]
        return pd.Series(ridge.predict(X), index=X.index).unstack()

    def normalize(self):
        self.data[:, 2:-1] = self.data[:, 2:-1].fillna(0)


class LassoCombiner(AlphaCombinerBase):

    def __init__(self, periods, alpha, fit_intercept=False, **kwargs):
        super(LassoCombiner, self).__init__(periods, **kwargs)
        self.alpha = alpha
        self.fit_intercept = fit_intercept

    def fit(self, X, Y):
        X, Y = X.ix[Y.notnull()], Y.ix[Y.notnull()]
        ridge = skl.Lasso(alpha=self.alpha, fit_intercept=self.fit_intercept)
        ridge.fit(X, Y)
        X = self.data.iloc[:, 2:-1]
        return pd.Series(ridge.predict(X), index=X.index).unstack()

    def normalize(self):
        self.data[:, 2:-1] = self.data[:, 2:-1].fillna(0)


class ElasticNetCombiner(AlphaCombinerBase):

    def __init__(self, periods, alpha, rho, fit_intercept=False, **kwargs):
        super(ElasticNetCombiner, self).__init__(periods, **kwargs)
        self.alpha = alpha
        self.rho = rho
        self.fit_intercept = fit_intercept

    def fit(self, X, Y):
        X, Y = X.ix[Y.notnull()], Y.ix[Y.notnull()]
        elastic = skl.ElasticNet(alpha=self.alpha, l1_ratio=self.rho, fit_intercept=self.fit_intercept)
        elastic.fit(X, Y)
        X = self.data.iloc[:, 2:-1]
        return pd.Series(elastic.predict(X), index=X.index).unstack()

    def normalize(self):
        self.data[:, 2:-1] = self.data[:, 2:-1].fillna(0)
