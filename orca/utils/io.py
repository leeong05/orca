"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd

from orca.operation.api import format

def read_frame(fname, ftype='csv'):
    if ftype == 'csv':
        return format(pd.read_csv(fname, header=0, parse_dates=[0], index_col=0))
    elif ftype == 'pickle':
        return pd.read_pickle(fname)
    elif ftype == 'msgpack':
        return pd.read_msgpack(fname)

def dump_frame(df, fname, ftype='csv'):
    with open(fname, 'w') as file:
        if ftype == 'csv':
            df.to_csv(file)
        elif ftype == 'pickle':
            df.to_pickle(file)
        elif ftype == 'msgpack':
            df.to_msgpack(file)
