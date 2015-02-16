"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from collections import OrderedDict
import abc

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
        alpha.index = pd.to_datetime(alpha.index)
        self.name_alpha[name] = alpha
        self.info('Added alpha {}'.format(name))

    def __setitem__(self, name, alpha):
        """Convenient method wrapper of :py:meth:`add_alpha`."""
        self.add_alpha(name, alpha)
        self.info('Added alpha {}'.format(name))

    def prepare_data(self):
        """Prepare inputs for regression."""
        X = pd.Panel.from_dict(self.name_alpha, intersect=False)
        X = X.to_frame(filter_observations=False).dropna(how='all')
        X.index.names = ['date', 'sid']
        self.data = X.reset_index()
        self.data.index = X.index

        startdate, enddate = self.data['date'].min().strftime('%Y%m%d'), self.data['date'].max().strftime('%Y%m%d')
        Y = self.quote.fetch('returnsN', self.periods, startdate, enddate, self.periods)
        Y = Y.shift(-self.periods).iloc[self.periods:]
        Y = Y.stack().ix[X.index]
        self.data['returns'] = Y

        self.data = self.data.ix[Y.notnull()]
        self.info('Data prepared')

    def get_XY(self, start=None, end=None):
        data = self.data
        if start is not None:
            data = data.query('date >= {!r}'.format(str(start)))
        if end is not None:
            data = data.query('date <= {!r}'.format(str(end)))
        X, Y = data.iloc[:, 2:-1], data.iloc[:, -1]
        return X, Y

    @abc.abstractmethod
    def normalize(self):
        raise NotImplementedError

    @abc.abstractmethod
    def fit(self, X, Y):
        raise NotImplementedError
