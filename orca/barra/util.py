"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from string import Template

import pandas as pd

def read_csv(path, **kwargs):
    index_col = kwargs.pop('index_col', None)
    default_kwargs = {
        'header': 0,
        'dtype': {0: str},
        }
    default_kwargs.update(kwargs)
    df = pd.read_csv(path, **default_kwargs)
    if index_col is None:
        return df
    col = df.columns[index_col]
    df.index = df[col]
    del df[col]
    return df

def expand_portfolio(original, composite_dfs, sid_bid={}, compact=False):
    long = original.query('weight > 0 & type == "eREGULAR"').copy()
    short = original.query('weight < 0 & type == "eREGULAR"').copy()
    long_weight, short_weight = long['weight'].to_dict(), short['weight'].to_dict()
    composites = original.query('type == "eCOMPOSITE"')
    for composite_id, w in composites['weight'].iteritems():
        composite = composite_dfs[composite_id].copy()
        composite['weight'] *= w
        if w > 0:
            for sid, weight in composite['weight'].iteritems():
                long_weight[sid] = long_weight.get(sid, 0) + weight
        else:
            for sid, weight in composite['weight'].iteritems():
                short_weight[sid] = short_weight.get(sid, 0) + weight
    long_weight, short_weight = pd.Series(long_weight), pd.Series(short_weight)

    if not compact:
        long_weight, short_weight = pd.DataFrame({'weight': long_weight}), pd.DataFrame({'weight': short_weight})
        long_weight['bid'] = [sid_bid[sid] for sid in long_weight.index]
        short_weight['bid'] = [sid_bid[sid] for sid in short_weight.index]
        long_weight = long_weight.reindex(columns=['bid', 'weight'])
        short_weight = short_weight.reindex(columns=['bid', 'weight'])
        long_weight.index.name, short_weight.index.name = 'sid', 'sid'
        return long_weight, short_weight

    sids = long_weight.index.union(short_weight.index)
    weight = long_weight.reindex(index=sids).fillna(0) + short_weight.reindex(index=sids).fillna(0)
    weight = pd.DataFrame({'weight': weight})
    weight['bid'] = [sid_bid[sid] for sid in weight.index]
    weight = weight.reindex(columns=['bid', 'weight'])
    weight.index.name = 'sid'
    return weight

def compact_portfolio(df1, df2):
    w1, w2 = df1['weight'], df2['weight']
    sids = w1.index.union(w2.index)
    sid_bid = df1['bid'].to_dict()
    sid_bid.update(df2['bid'].to_dict())
    df = pd.DataFrame({'weight': w1.reindex(index=sids).fillna(0) + w2.reindex(index=sids).fillna(0)})
    df['bid'] = [sid_bid[sid] for sid in sids]
    df = df.reindex(columns=['bid', 'weight'])
    return df

def parse_bool(text, default=None):
    if text in (True, False):
        return text
    if text.lower() in ('true', 'false'):
        return text.lower() == 'true'
    try:
        return bool(int(text))
    except ValueError, e:
        if default is not None:
            return default
        raise e

def generate_path(s, date, **kwargs):
    if isinstance(s, str):
        s = Template(s)
    return s.substitute(YYYYMMDD=date, YYYYMM=date[:6], YYYY=date[:4], MM=date[4:6], DD=date[6:8], **kwargs)

def set_asset_attribute(func, val, type, predicate=None):
    try:
        val = type(val)
        if predicate(val):
            func(val)
    except:
        return
