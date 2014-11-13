"""
.. moduleauthor: Li, Wang <wangziqi@foreseefund.com>
"""

import os
#import time
import shutil
import tempfile

import numpy as np
import pandas as pd

import unittest

from orca.data.csv import (
        CSVSaver,
        CSVLoader,
        )
from orca.utils.testing import frames_equal


class CSVDataTestCase(unittest.TestCase):

    def setUp(self):
        self.dir = os.path.join(tempfile.mkdtemp(), 'cache')
        self.dates = pd.date_range('20140101', '20140111')
        self.data = pd.DataFrame(np.random.randn(len(self.dates), 2), index=self.dates)
        self.saver = CSVSaver(self.dir, debug_on=False)
        self.loader = CSVLoader(self.dir, debug_on=False)

    def tearDown(self):
        self.dates = None
        self.data = None
        self.saver, self.loader = None, None
        shutil.rmtree(self.dir)

    def test_saver_setitem(self):
        self.saver['data'] = self.data
        fname = self.saver.datafiles['data']
        self.assertTrue(os.path.exists(fname))

    """
    def test_saver_setitem_overwritten(self):
        self.saver['data'] = self.data
        fname = self.saver.datafiles['data']
        self.saver['data'] = self.data
    """

    def test_saver_setitem_none(self):
        self.saver['data'] = self.data
        fname = self.saver.datafiles['data']
        self.saver['data'] = None
        self.assertFalse(os.path.exists(fname))

    def test_saver_delitem(self):
        self.saver['data'] = self.data
        fname = self.saver.datafiles['data']
        del self.saver['data']
        self.assertFalse(os.path.exists(fname))

    def test_loader_getitem(self):
        self.saver['data'] = self.data
        self.loader.configure(parse_dates=True)
        data = self.loader['data']
        data.index = self.data.index
        data.columns = self.data.columns
        self.assertTrue(frames_equal(data, self.data))

    def test_loader_load_once(self):
        self.saver['data'] = self.data
        self.loader.configure(parse_dates=True)
        data1 = self.loader['data']
        self.saver['data'] = None
        data2 = self.loader['data']
        self.assertTrue(data1 is data2)
