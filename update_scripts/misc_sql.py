import os

tradable_dir = '/data/HFData/daily/tradable'

def gp_tradable(date):
    return os.path.join(tradable_dir, date[:4], date[4:6], 'valid.'+date)
