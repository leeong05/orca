"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
import magic

def read_frame(fname, ftype=None, return_ftype=False):
    if ftype is None:
        with magic.Magic() as m:
            ftype = m.id_filename(fname)
            if ftype[:4] == 'data':
                ftype = 'msgpack'
            elif ftype[:10] == 'ASCII text':
                ftype = 'csv'
            elif ftype[:4] == '8086':
                ftype = 'pickle'
            else:
                ftype = None
    if ftype == 'msgpack':
        df = pd.read_msgpack(fname)
    elif ftype == 'csv':
        df = pd.read_csv(fname, header=0, parse_dates=[0], index_col=0)
    elif ftype == 'pickle':
        df = pd.read_pickle(fname)
    if ftype is not None:
        return (df, ftype) if return_ftype else df
    raise Exception('File type not recognized for {}'.format(fname))

def dump_frame(df, fname, ftype='csv'):
    with open(fname, 'w') as file:
        if ftype == 'csv':
            df.to_csv(file)
        elif ftype == 'pickle':
            df.to_pickle(file)
        elif ftype == 'msgpack':
            df.to_msgpack(file)
