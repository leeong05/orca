"""
.. moduleauthor: Li, Wang <wangziqi@foreseefund.com>
"""

import os
import shutil
import tempfile

import logging
import unittest

from orca.data import (
        IOBase,
        SaverBase,
        LoaderBase)


class IOBaseDummy(IOBase):

    def save(self, name, data, **kwargs):
        raise NotImplementedError

    def load(self, name, **kwargs):
        raise NotImplementedError


class SaverBaseDummy(SaverBase):

    def save(self, name, data, **kwargs):
        raise NotImplementedError


class LoaderBaseDummy(LoaderBase):

    def load(self, name, **kwargs):
        raise NotImplementedError


class DataTestCase(unittest.TestCase):

    def setUp(self):
        self.io = IOBaseDummy('')
        self.dir = tempfile.mkdtemp()
        self.saver = SaverBaseDummy(os.path.join(self.dir, 'cache'))
        self.loader = LoaderBaseDummy(self.dir)

    def tearDown(self):
        self.io = None
        self.saver = None
        self.loader = None
        shutil.rmtree(self.dir)

    def test_iobase_is_abstract_class(self):
        self.assertRaises(TypeError, IOBase)

    def test_saverbase_is_abstract_class(self):
        self.assertRaises(TypeError, SaverBase)

    def test_loaderbase_is_abstract_class(self):
        self.assertRaises(TypeError, LoaderBase)

    def test_logger_name(self):
        self.assertEqual(self.io.logger.name, IOBase.LOGGER_NAME)

    def test_debug_mode_default_on(self):
        self.assertEqual(self.io.logger.level, logging.DEBUG)

    def test_saver_load_exception(self):
        self.assertRaises(NotImplementedError, self.saver.load, '')

    def test_loader_save_exception(self):
        self.assertRaises(NotImplementedError, self.loader.save, '', '')

    def test_saver_cachedir_created(self):
        self.assertTrue(os.path.exists(self.saver.cachedir))

    def test_loader_cachedir_exception(self):
        self.assertRaises(IOError, LoaderBaseDummy, os.path.join(self.dir, 'cached'))
