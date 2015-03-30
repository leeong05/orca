"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca import SIDS
from orca.mongo.industry import IndustryFetcher
FETCHER = IndustryFetcher()

def get_board(sid):
    if sid[:2] == '60':
        return 'SH'
    elif sid[:2] == '30':
        return 'CYB'
    elif sid[:3] == '002':
        return 'ZXB'
    return 'SZ'

BOARD = pd.Series({sid: get_board(sid) for sid in SIDS})

def group_by_board(df):
    if isinstance(df, pd.Series):
        group = BOARD.ix[df.index]
        return df.groupby(group)
    group = BOARD.ix[df.columns]
    return df.groupby(group, axis=1)

def group_by_industry(df, industry='sector', standard='SW2014', date=None, use_name=False):
    industry = FETCHER.fetch_daily(industry, date=date).dropna()
    if use_name:
        ind_name = FETCHER.fetch_info('name')
        industry = industry.map(lambda x: ind_name[x])
    if isinstance(df, pd.Series):
        sids = industry.index.intersection(df.index)
        return df.ix[sids].groupby(industry.ix[sids])
    sids = industry.index.intersection(df.columns)
    return df[sids].groupby(industry.ix[sids], axis=1)
