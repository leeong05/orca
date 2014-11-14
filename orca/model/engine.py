"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
from sqlalchemy import create_engine

URI = 'mysql+mysqldb://{user}:{password}@localhost/alpha'.format(user='wang', password='lwang')
engine = create_engine(URI)

def fetch_sql(sql):
    return pd.read_sql_query(sql)
