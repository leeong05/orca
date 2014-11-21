"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd
import warnings
warnings.simplefilter(action = "ignore", category = pd.io.pytables.PerformanceWarning)

from base import (
        SaverBase,
        LoaderBase,
        )


class HDFSaver(SaverBase):
    """Class for saving data in HDF5 format files. It uses HDFStore from Pandas library internally."""

    def __init__(self, cachedir, **kwargs):
        kwargs.update({'plain': False})
        super(HDFSaver, self).__init__(cachedir, **kwargs)
        dirname = os.path.dirname(self.cachedir)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.store = pd.HDFStore(self.cachedir)

    def save(self, name, data):
        self.store[name] = data
        self.store.flush()
        fname = os.path.join(self.cachedir, name)
        self.debug('Saved data in file: {0!r}'.format(fname))
        self.datafiles[name] = name

    def delete(self, fname):
        del self.store[fname]
        self.debug('Removed file: {0!r}'.format(fname))
        self.store.flush()

    def __del__(self):
        self.store.close()


class HDFLoader(LoaderBase):
    """Class for loading data in HDF5 format files."""

    def __init__(self, cachedir, **kwargs):
        super(HDFLoader, self).__init__(cachedir, **kwargs)
        self.store = pd.HDFStore(self.cachedir)

    def load(self, name):
        return self.store[name]

    def __del__(self):
        self.store.close()
