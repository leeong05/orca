"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from collections import OrderedDict
from datetime import datetime
import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm

from orca import logger
from orca import DATES
from orca.mongo.quote import QuoteFetcher
from orca.operation import api


class AlphaCombiner(object):
    """Class to combine alphas.

    :param int periods: How many days of returns as the predicted variable?
    :param float quantile: Only use tailed returnsN as dependent variable and other observations will be discarded. Default: None, no discard

    """
    LOGGER_NAME = 'combiner'

    def __init__(self, periods, quantile=None, debug_on=True):
        self.logger = logger.get_logger(AlphaCombiner.LOGGER_NAME)
        self.set_debug_mode(debug_on)
        self.periods = periods
        self.quantile = quantile
        self.quote = QuoteFetcher(datetime_index=True, reindex=True)
        self.name_alpha = OrderedDict()
        self.weight = None
        self.is_start, self.is_end = None, None
        self.os_start, self.os_end = None, None
        self.X, self.Y, self.W = None, None, None
        self.oX, self.oY = None, None
        self.result = None

    def set_debug_mode(self, debug_on):
        """Enable/Disable debug level message in data fetchers.
        This is enabled by default."""
        level = logging.DEBUG if debug_on else logging.INFO
        self.logger.setLevel(level)

    def dump(self, fpath):
        """Save predicted result in file."""
        with open(fpath, 'w') as file:
            self.oY.to_csv(file)
        self.logger.info('Predicted alphas is saved in %s', fpath)

    def add_alpha(self, name, alpha):
        """
        :param DataFrame alpha: Alpha to be added

        """
        self.name_alpha[name] = api.format(alpha)

    def __setitem__(self, name, alpha):
        """Convenient method wrapper of :py:meth:`add_alpha`."""
        self.add_alpha(name, alpha)

    def set_weight(self, weight):
        W = api.format(weight)
        self.weight = W[np.isfinite(W) & (W > 0)]

    def set_isdates(self, start=None, end=None):
        self.is_start, self.is_end = start, end

    def set_osdates(self, start=None, end=None):
        self.os_start, self.os_end = start, end

    def prepare_XYW(self):
        """Prepare inputs for regression."""
        X = pd.Panel.from_dict(self.name_alpha, intersect=True)
        if self.os_start is not None:
            index = [dt for dt in self.X.major_axis if dt >= datetime.strptime(str(self.os_start), '%Y%m%d')]
            X = X.reindex(major_axis=index)
        if self.os_end is not None:
            index = [dt for dt in self.X.major_axis if dt <= datetime.strptime(str(self.os_end), '%Y%m%d')]
            X = X.reindex(major_axis=index)
        self.oX = X.to_frame(filter_observations=False).dropna(how='all').fillna(0)
        self.logger.debug('Out-of-sample cases ready')

        X = pd.Panel.from_dict(self.name_alpha, intersect=True)
        if self.is_start is not None:
            index = [dt for dt in self.X.major_axis if dt >= datetime.strptime(str(self.is_start), '%Y%m%d')]
            X = X.reindex(major_axis=index)
        if self.is_end is not None:
            index = [dt for dt in self.X.major_axis if dt <= datetime.strptime(str(self.is_end), '%Y%m%d')]
            X = X.reindex(major_axis=index)
        self.logger.debug('In-sample cases ready')

        startdate, enddate = X.major_axis[0].strftime('%Y%m%d'), X.major_axis[-1].strftime('%Y%m%d')
        edi = DATES.index(enddate)
        if edi + self.periods > len(DATES)-1:
            edi = len(DATES)-1
            self.logger.warning('Some recent observations may not have dependent variables; these will be removed from regression')
        else:
            edi = edi + self.periods
        Y = self.quote.fetch('returnsN', self.periods, startdate, DATES[edi]).shift(-self.periods)
        Y = Y.ix[X.major_axis]
        if isinstance(self.quantile, float):
            Y = api.qtop(Y, self.quantile) - api.qbottom(Y, self.quantile)

        if self.weight is None:
            W = pd.DataFrame(1, index=Y.index, columns=Y.columns)
            self.logger.debug('No weight matrix specified, default to equal weight')
        else:
            W = self.weight.ix[Y.index]

        names = self.name_alpha.keys() + ['returns', 'weight']
        tmp = {}
        for name in self.name_alpha.keys():
            tmp[name] = X[name]
        tmp['returns'], tmp['weight'] = Y, W
        XYW = pd.Panel.from_dict(tmp, intersect=True).reindex(items=names).to_frame(filter_observations=False)
        X = XYW.iloc[:, :-2].dropna(how='all')
        Y = XYW.iloc[:, -2].dropna()
        index = X.index.intersection(Y.index)
        XYW = XYW.reindex(index=index).fillna(0)

        self.X, self.Y, self.W = XYW.iloc[:,:-2], XYW.iloc[:, -2], XYW.iloc[:, -1]
        self.logger.debug('Regression model inputs ready')

    def fit(self):
        model = sm.WLS(self.Y, self.X, self.W)
        self.result = model.fit()
        self.logger.debug('Model fitting done')

    def predict(self):
        self.oY = pd.Series(self.result.predict(self.oX), index=self.oX.index)
        self.oY = api.format(self.oY.unstack()) * 242. / self.periods
        self.logger.debug('Model prediction done')

    def run(self, fpath):
        """Main interface.

        :param str fpath: Valid file path.

        """
        self.prepare_XYW()
        self.fit()
        self.logger.info('\nR^2: %f\n%s', self.result.rsquared, self.result.summary())
        self.predict()
        self.dump(fpath)
