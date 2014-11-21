"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc
import logging

from orca import logger


class OperationBase(object):
    """Base class for alpha operations.

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'operation'

    def __init__(self, debug_on=True):
        self.logger = logger.get_logger(OperationBase.LOGGER_NAME)
        self.set_debug_mode(debug_on)

    @abc.abstractmethod
    def operate(self, alpha):
        """Override (**mandatory**) to operate on alphas.

        **This is the sole interface of an operation class**.

        :param DataFrame alpha: Alpha to be operated on
        :raises: NotImplementedError
        """
        raise NotImplementedError

    def set_debug_mode(self, debug_on):
        """Enable/Disable debug level messages in the operation.
        This is enabled by default."""
        level = logging.DEBUG if debug_on else logging.INFO
        self.logger.setLevel(level)

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
