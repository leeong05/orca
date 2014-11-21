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

    def _save(self, name, data, **kwargs):
        raise NotImplementedError

    def _load(self, name, **kwargs):
        raise NotImplementedError

    def _delete(self, fnmae):
        raise NotImplementedError


class SaverBaseDummy(SaverBase):

    def _save(self, name, data, **kwargs):
        raise NotImplementedError

    def _delete(self, fnmae):
        raise NotImplementedError


class LoaderBaseDummy(LoaderBase):

    def _load(self, name, **kwargs):
        raise NotImplementedError


class DataTestCase(unittest.TestCase):

    def setUp(self):
        self.io = IOBaseDummy('')
        self.dir = tempfile.mkdtemp()
        self.saverplain = SaverBaseDummy(os.path.join(self.dir, 'cache'))
        self.savernon = SaverBaseDummy(os.path.join(self.dir, 'noncache'), plain=False)
        self.loader = LoaderBaseDummy(self.dir)

    def tearDown(self):
        self.io = None
        self.saverplain = None
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
        self.assertRaises(NotImplementedError, self.saverplain._load, '')

    def test_loader_save_exception(self):
        self.assertRaises(NotImplementedError, self.loader._save, '', '')

    def test_plain_saver_cachedir_created(self):
        self.assertTrue(os.path.exists(self.saverplain.cachedir))

    def test_nonplain_saver_cachedir_not_created(self):
        self.assertFalse(os.path.exists(self.savernon.cachedir))

    def test_loader_cachedir_exception(self):
        self.assertRaises(IOError, LoaderBaseDummy, os.path.join(self.dir, 'cached'))
