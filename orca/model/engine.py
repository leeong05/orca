"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from sqlalchemy import create_engine

URI = 'mysql+mysqldb://{user}:{password}@localhost/alpha'.format(user='wang', password='lwang')
engine = create_engine(URI)
