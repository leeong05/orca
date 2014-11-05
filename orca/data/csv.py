"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os

import pandas as pd

from orca.data import (
        SaverBase,
        LoaderBase)


class CSVSaver(SaverBase):
    """Class for saving data in csv format files."""

    def save(self, name, data, **kwargs):
        data.to_csv(os.path.join(self.cachedir, name), **kwargs)


class CSVLoader(LoaderBase):
    """Class for saving data in csv format files."""

    def load(self, name, **kwargs):
        pd.read_csv(os.path.join(self.cachedir, name), **kwargs)
