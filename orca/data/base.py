"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
import abc
import logging

from orca import logger

class IOBase(object):
    """Base class for data savers/loaders.

    :param str cachedir: Cache diretory path

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'data'

    def __init__(self, cachedir, debug_on=True):
        self.cachedir = cachedir
        self.logger = logger.get_logger(IOBase.LOGGER_NAME)
        self.set_debug_mode(debug_on)

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
        :raises: NotImplementedError

        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, fname):
        """Override (**mandatory**) to delete data.

        :param str fname: File name on disk
        :raises: NotImplementedError

        """
        raise NotImplementedError

    @abc.abstractmethod
    def load(self, name, **kwargs):
        """Override (**mandatory**) to load data.

        :param str name: File name of the saved data on disk
        :raises: NotImplementedError

        """
        raise NotImplementedError


class SaverBase(IOBase):
    """Base class for data savers.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, cachedir, **kwargs):
        super(SaverBase, self).__init__(cachedir, **kwargs)
        self.plain = kwargs.get('plain', True)
        if self.plain and not os.path.exists(self.cachedir):
            os.makedirs(self.cachedir)
        self.params = {}
        self.datafiles = {}

    def configure(self, **kwargs):
        self.params.update(kwargs)

    def load(self, name, **kwargs):
        """This method should never be called by a data saver.

        :raises: NotImplementedError

        """
        raise NotImplementedError

    def delete(self, fname):
        """Simple-minded file deletion.
        Override (**mandatory**) for any non plain file formats.
        """
        if not self.plain:
            raise NotImplementedError

        try:
            os.remove(fname)
            self.debug('Removed file: {0!r}'.format(fname))
        except IOError:
            self.warning('Failed to remove file: {0!r}'.format(fname))

    def __setitem__(self, key, val):
        """Covenient helper special method.

        :param val: When it is None, this is equivalent to deleting the data file with name ``key``

        """
        if val is None:
            del self[key]
            return

        self.save(key, val)

    def __delitem__(self, key):
        """Convenient helper special method."""
        if key not in self.datafiles:
            self.warning('No such data with name {0!r} exists'.format(key))
            return

        self.delete(self.datafiles[key])
        del self.datafiles[key]


class LoaderBase(IOBase):
    """Base class for data loaders.

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, cachedir, **kwargs):
        super(LoaderBase, self).__init__(cachedir, **kwargs)
        if not os.path.exists(self.cachedir):
            raise IOError('No cache exists with path {0!r}'.format(self.cachedir))
        self.datas = {}

    def configure(self, **kwargs):
        """Method to pass parameters to file parse function internally used."""
        self.params.update(kwargs)

    def save(self, name, data, **kwargs):
        """This method should never be called by a data loader.

        :raises: NotImplementedError

        """
        raise NotImplementedError

    def delete(self, fname):
        """This method should never be called by a data loader.

        :raises: NotImplementedError

        """
        raise NotImplementedError

    def __getitem__(self, key):
        """Convenient helper function. This avoids loading the data from disk over and over again."""
        if key in self.datas:
            return self.datas[key]

        data = self.load(key)
        self.datas[key] = data
        return data
