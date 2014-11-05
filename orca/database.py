"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from pymongo import MongoClient

mongo = MongoClient('192.168.1.183')
db = mongo.stocks
db.authenticate('kiwi', '0p;/9ol.')

dates = db.dates.distinct('date')
sids = db.sids.distinct('sid')
