"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd

from base import (
        SaverBase,
        LoaderBase,
        )


class PickleSaver(SaverBase):
    """Class for saving data in pickled files."""

    def save(self, name, data):
        """Use ``kwargs`` to pass additional file writing parameters or override the default setting.

        .. seealso:: :py:meth:`orca.data.base.SaverBase.configure`
        """
        fname = os.path.join(self.cachedir, name)
        if os.path.exists(fname):
            self.warning('Existing file will be overwritten: {0!r}'.format(fname))
        data.to_pickle(fname)
        self.debug('Saved data in file: {0!r}'.format(fname))
        self.datafiles[name] = fname


class PickleLoader(LoaderBase):
    """Class for loading data in pickled files."""

    def __init__(self, cachedir, **kwargs):
        super(PickleLoader, self).__init__(cachedir, **kwargs)
        self.postfix = kwargs.get('postfix', '')

    def load(self, name):
        return pd.read_pickle(os.path.join(self.cachedir, name + self.postfix))


class MsgPackSaver(SaverBase):
    """Class for saving data in msgpack files."""

    def save(self, name, data):
        """Use ``kwargs`` to pass additional file writing parameters or override the default setting.

        .. seealso:: :py:meth:`orca.data.base.SaverBase.configure`
        """
        fname = os.path.join(self.cachedir, name)
        if os.path.exists(fname):
            self.warning('Existing file will be overwritten: {0!r}'.format(fname))
        data.to_msgpack(fname)
        self.debug('Saved data in file: {0!r}'.format(fname))
        self.datafiles[name] = fname


class MsgPackLoader(LoaderBase):
    """Class for loading data in msgpack files."""

    def __init__(self, cachedir, **kwargs):
        super(MsgPackLoader, self).__init__(cachedir, **kwargs)
        self.postfix = kwargs.get('postfix', '')

    def load(self, name):
        return pd.read_msgpack(os.path.join(self.cachedir, name + self.postfix))
