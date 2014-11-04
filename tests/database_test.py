"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import unittest

from pymongo.errors import OperationFailure

from orca.database import db, dates, sids


class MongoDBTestCase(unittest.TestCase):

    def test_correct_db_connected(self):
        self.assertEqual(db.name, 'stocks_dev')

    def test_is_authorized(self):
        self.assertIsInstance(db.collection_names(), list)

    def test_db_logout(self):
        db.logout()
        self.assertRaises(OperationFailure, db.db.collection_names)
        db.authenticate('stocks_dev', 'stocks_dev')

    def test_dates(self):
        self.assertListEqual(db.dates.distinct('date'), dates)

    def test_sids(self):
        self.assertListEqual(db.sids.distinct('sid'), sids)
