"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.industry import IndustryFetcher
industry_fetcher = IndustryFetcher()

def industry2name(pdobj):
    pdobj = pdobj.copy()
    ind_name = industry_fetcher.fetch_info()
    pdobj.index = [ind_name[ind] for ind in pdobj.index]
    return pdobj

def name2industry(pdobj):
    pdobj = pdobj.copy()
    name_ind = {v: k for k, v in industry_fetcher.fetch_info().iteritems()}
    pdobj.index = [name_ind[name] for name in pdobj.index]
    return pdobj

import pandas as pd

from orca import DATES
from orca.mongo.quote import QuoteFetcher
quote_fetcher = QuoteFetcher(datetime_index=True)

import dateutil

def fetch_returns(dt_index, rshift, lshift=-1):
    res = {}
    for dt, date in zip(dt_index, dateutil.to_datestr(dt_index)):
        di, date = dateutil.parse_date(DATES, date, -1)
        if di-lshift < 0 or di+rshift+1 > len(DATES):
            continue
        r = quote_fetcher.fetch_window('returns', DATES[di-lshift: di+rshift+1])
        res[dt] = (1+r).cumprod()[-1]-1.
    res = pd.DataFrame(res).T
    return res

def fetch_dates(df, dt_index, rshift=0, lshift=0):
    df_dates = dateutil.to_datestr(dt_index)
    res = []
    for dt, date in zip(dt_index, dateutil.to_datestr(dt_index)):
        try:
            di, date = dateutil.parse_date(df_dates, date, -1)
            assert di-lshift >= 0 and di+rshift+1 <= len(df_dates)
            if rshift+lshift == 0:
                res[dt] = df.iloc[di-lshift]
            else:
                res[dt] = df.iloc[di-lshift:di+rshift+1]
        except ValueError:
            pass
        else:
            raise
    if rshift+lshift == 0:
        res = pd.DataFrame(res).T
    return res
