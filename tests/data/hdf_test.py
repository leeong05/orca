"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
import tempfile

import numpy as np
import pandas as pd

import unittest

from orca.data.hdf import (
        HDFSaver,
        HDFLoader,
        )
from orca.utils.testing import frames_equal


class HDFDataTestCase(unittest.TestCase):

    def setUp(self):
        self.h5file = os.path.join(tempfile.mkdtemp(), 'test.h5')
        self.dates = pd.date_range('20140101', '20140111')
        self.data = pd.DataFrame(np.random.randn(len(self.dates), 2), index=self.dates)
        self.saver = HDFSaver(self.h5file, debug_on=True)
        self.loader = HDFLoader(self.h5file, debug_on=True)

    def tearDown(self):
        self.dates = None
        self.data = None
        self.saver, self.loader = None, None
        os.remove(self.h5file)

    def test_saver_setitem(self):
        self.saver['data'] = self.data
        name = self.saver.datafiles['data']
        self.assertTrue('/' + name in self.saver.store.keys())

    def test_saver_setitem_none(self):
        self.saver['data'] = self.data
        name = self.saver.datafiles['data']
        self.saver['data'] = None
        self.assertFalse('/'+name in self.saver.store.keys())

    def test_saver_delitem(self):
        self.saver['data'] = self.data
        name = self.saver.datafiles['data']
        del self.saver['data']
        self.assertFalse('/'+name in self.saver.store.keys())

    def test_loader_getitem(self):
        self.saver['data'] = self.data
        data = self.loader['data']
        self.assertTrue(frames_equal(data, self.data))

    def test_loader_load_once(self):
        self.saver['data'] = self.data
        data1 = self.loader['data']
        data2 = self.loader['data']
        self.assertTrue(data1 is data2)
