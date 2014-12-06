"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc

import logbook
logbook.set_datetime_format('local')


class OperationBase(object):
    """Base class for alpha operations.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'operation'

    def __init__(self):
        self.logger = logbook.Logger(OperationBase.LOGGER_NAME)

    @abc.abstractmethod
    def operate(self, alpha):
        """Override (**mandatory**) to operate on alphas.

        **This is the sole interface of an operation class**.

        :param DataFrame alpha: Alpha to be operated on
        :raises: NotImplementedError
        """
        raise NotImplementedError

    def debug(self, msg):
        """Logs a message with level DEBUG on the operation logger."""
        self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the operation logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the operation logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the operation logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the operation logger."""
        self.logger.critical(msg)
