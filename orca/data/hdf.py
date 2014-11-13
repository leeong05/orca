"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

from pandas import HDFStore

from base import (
        SaverBase,
        LoaderBase,
        )


class HDFSaver(SaverBase):
    """Class for saving data in HDF5 format files."""

    def __init__(self, cachedir, **kwargs):
        kwargs.update({'plain': False})
        super(HDFSaver, self).__init__(cachedir, **kwargs)
        dirname = os.path.dirname(self.cachedir)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.store = HDFStore(self.cachedir)

    def _save(self, name, data):
        self.store[name] = data
        self.store.flush()
        fname = os.path.join(self.cachedir, name)
        self.debug('Saved data in file: {0!r}'.format(fname))
        self.datafiles[name] = name

    def _delete(self, name):
        del self.store[name]
        self.debug('Removed file: {0!r}'.format(name))
        self.store.flush()

    def __del__(self):
        self.store.close()


class HDFLoader(LoaderBase):
    """Class for loading data in HDF5 format files."""

    def __init__(self, cachedir, **kwargs):
        super(HDFLoader, self).__init__(cachedir, **kwargs)
        self.store = HDFStore(self.cachedir)

    def _load(self, name):
        return self.store[name]

    def __del__(self):
        self.store.close()
