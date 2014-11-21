"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from pymongo import MongoClient

mongo = MongoClient('192.168.1.183')
db = mongo.stocks_dev
db.authenticate('stocks_dev', 'stocks_dev')

dates = db.dates.distinct('date')
sids = db.sids.distinct('sid')
