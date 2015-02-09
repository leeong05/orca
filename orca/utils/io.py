"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
import magic

def read_frame(fname, ftype=None):
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
        return pd.read_msgpack(fname)
    elif ftype == 'csv':
        return pd.read_csv(fname, header=0, parse_dates=[0], index_col=0)
    elif ftype == 'pickle':
        return pd.read_pickle(fname)
    raise Exception('File type not recognized for {}'.format(fname))
