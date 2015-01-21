srcdir1 = '/home/SambaServer/TinySoftData/'
srcdir2 = '/home/SambaServer/extend_data/TinySoftData/'
col_index = [0, 2, 4, 5, 6, 7, 8, 9, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 37, 38, 39, 40, 41, 42]
col_names = ['sid', 'datetime', 'open', 'close', 'high', 'low', 'volume', 'amount', 'bid1', 'bid2', 'bid3', 'bid4', 'bid5', 'ask1', 'ask2', 'ask3', 'ask4', 'ask5', 'bds1', 'bds2', 'bds3', 'bds4', 'bds5', 'aks1', 'aks2', 'aks3', 'aks4', 'aks5', 'bvolume', 'bamount', 'svolume', 'samount', 'iwbds', 'iwaks']
dnames = ['open', 'close', 'high', 'low', 'volume', 'amount', 'bid1', 'bid2', 'bid3',   'bid4', 'bid5', 'ask1', 'ask2', 'ask3', 'ask4', 'ask5', 'bds1', 'bds2', 'bds3', 'bds4', 'bds5', 'aks1', 'aks2', 'aks3', 'aks4', 'aks5', 'bvolume', 'bamount', 'svolume', 'samount', 'iwbds', 'iwaks', 'vwap']
index_dnames = ['open', 'close', 'high', 'low', 'volume', 'amount', 'vwap']

def is_stock(sid):
    market, ticker = sid[:2], sid[2:8]
    return (market == 'SH' and ticker[:2] == '60') or \
           (market == 'SZ' and ticker[:2] == '30') or \
           (market == 'SZ' and ticker[:2] == '00')
