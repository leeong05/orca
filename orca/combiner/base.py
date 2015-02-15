"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from collections import OrderedDict
import abc

import numpy as np
import pandas as pd
import logbook
logbook.set_datetime_format('local')

from orca.mongo.quote import QuoteFetcher


class AlphaCombinerBase(object):
    """Base class to combine alphas.

    :param int periods: How many days of returns as the predicted variable?

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'combiner'

    def __init__(self, periods, debug_on=True, **kwargs):
        self.periods = periods
        self.logger = logbook.Logger(AlphaCombinerBase.LOGGER_NAME)
        self.debug_on = debug_on
        self.quote = QuoteFetcher(datetime_index=True, reindex=True)
        self.name_alpha = OrderedDict()
        self.data = None
        self.__dict__.update(kwargs)

    def debug(self, msg):
        """Logs a message with level DEBUG on the alpha logger."""
        if self.debug_on:
            self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the alpha logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the alpha logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the alpha logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the alpha logger."""
        self.logger.critical(msg)

    def add_alpha(self, name, alpha, ftype=None):
        """
        :param DataFrame alpha: Alpha to be added
        """
        self.name_alpha[name] = alpha.stack()

    def __setitem__(self, name, alpha):
        """Convenient method wrapper of :py:meth:`add_alpha`."""
        self.add_alpha(name, alpha)

    def prepare_data(self):
        """Prepare inputs for regression."""
        X = pd.DataFrame(self.name_alpha)
        X.index.names = ['date', 'sid']
        X = X.dropna(axis=0, how='all')
        self.data = X.reset_index()
        self.data.index = X.index

        startdate, enddate = self.data['date'].min().strftime('%Y%m%d'), self.data['date'].max().strftime('%Y%m%d')
        Y = self.quote.fetch('returnsN', self.periods, startdate, enddate, self.periods)[np.unique(self.data['sid'])]
        Y = Y.shift(-self.periods).iloc[self.periods:].stack()
        Y = Y.ix[X.index]
        self.data['returns'] = Y

        self.data = self.data.ix[Y.notnull()]

    def get_XY(self, start=None, end=None):
        if start is not None:
            data = self.data.query('date >= {!r}'.format(str(start)))
        if end is not None:
            data = data.query('data <= {!r}'.format(str(end)))
        return data.iloc[:, 2:-1], data.iloc[:, -1]

    def normalize(self, X):
        return X.fillna(0)

    @abc.abstractmethod
    def fit(self, X, Y):
        raise NotImplementedError
