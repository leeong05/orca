"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd

from base import (
        SaverBase,
        LoaderBase,
        )


class CSVSaver(SaverBase):
    """Class for saving data in csv format files."""

    def save(self, name, data, **kwargs):
        fname = os.path.join(self.cachedir, name)
        if os.path.exists(fname):
            self.warning('Existing file will be overwritten: {0!r}'.format(fname))
        params = self.params.copy()
        params.update(kwargs)
        with open(fname, 'w') as file:
            data.to_csv(file, **params)
        self.debug('Saved data in file: {0!r}'.format(fname))
        self.datafiles[name] = fname


class CSVLoader(LoaderBase):
    """Class for loading data in csv format files."""

    def __init__(self, cachedir, **kwargs):
        super(CSVLoader, self).__init__(cachedir, **kwargs)
        self.params = {
                'header': 0,
                'parse_dates': False,
                'index_col': 0,
                }
        self.postfix = kwargs.get('postfix', '')

    def load(self, name, **kwargs):
        params = self.params.copy()
        params.update(kwargs)
        return pd.read_csv(os.path.join(self.cachedir, name + self.postfix), **params)
