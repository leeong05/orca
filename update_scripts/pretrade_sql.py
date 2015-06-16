srcdir1 = '/home/SambaServer/TinySoftData/PreTrading'
srcdir2 = '/home/SambaServer/extend_data/TinySoftData/PreTrading'
store_path = '/home/SambaServer/extend_data/TinySoftData/PreTrading/store.h5'
msgpack_path = '/home/SambaServer/extend_data/TinySoftData/PreTrading/{date}.msgpack'

col_names = ['sid', 'datetime', 'bid1', 'ask1', 'bds1', 'bds2', 'aks1', 'aks2']

def is_stock(sid):
    market, ticker = sid[:2], sid[2:8]
    return (market == 'SH' and ticker[:2] == '60') or \
           (market == 'SZ' and ticker[:2] == '30') or \
           (market == 'SZ' and ticker[:2] == '00')
