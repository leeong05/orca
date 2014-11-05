"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
import abc
import logging

from orca import logger

class IOBase(object):
    """Base class for data savers/loaders.

    :param: str cachedir: Cache diretory path
    :param boolean debug_on: Enable/Disable debug level messages. Default: True

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'data'

    def __init__(self, cachedir, **kwargs):
        self.cachedir = cachedir
        self.logger = logger.get_logger(IOBase.LOGGER_NAME)
        self.set_debug_mode(kwargs.get('debug_on', True))
        self.__dict__.update(kwargs)

    def set_debug_mode(self, debug_on):
        """Enable/Disable debug level message in data savers/loaders.
        This is enabled by default."""
        level = logging.DEBUG if debug_on else logging.INFO
        self.logger.setLevel(level)

    def debug(self, msg):
        """Logs a message with level DEBUG on the data logger."""
        self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the data logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the data logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the data logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the data logger."""
        self.logger.critical(msg)

    @abc.abstractmethod
    def save(self, name, data, **kwargs):
        """Override (**mandatory**) to save data.

        :param str name: File name of the saved data on disk
        :param data: The data to be saved
        """

        raise NotImplementedError

    @abc.abstractmethod
    def load(self, name, **kwargs):
        """Override (**mandatory**) to load data.

        :param str name: File name of the saved data on disk
        """

        raise NotImplementedError


class SaverBase(IOBase):
    """Base class for data savers.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, cachedir, **kwargs):
        IOBase.__init__(self, cachedir, **kwargs)
        if not os.path.exists(self.cachedir):
            os.makedirs(self.cachedir)

    def load(self, name, **kwargs):
        """This method should never be called by a data saver."""
        raise NotImplementedError


class LoaderBase(IOBase):
    """Base class for data loaders.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, cachedir, **kwargs):
        IOBase.__init__(self, cachedir, **kwargs)
        if not os.path.exists(self.cachedir):
            raise IOError('No cache exists with path {0!r}'.format(self.cachedir))

    def save(self, name, data, **kwargs):
        """This method should never be called by a data loader."""
        raise NotImplementedError
