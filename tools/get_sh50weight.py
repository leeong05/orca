"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.components import ComponentsFetcher

components = ComponentsFetcher(as_bool=False)

def get_weight(start, end):
    hs300 = components.fetch('HS300', start, end)
    sh50 = components.fetch('SH50', start, end)
    hs300_sh50 = hs300[sh50.notnull()]
    return hs300_sh50.sum(axis=1)

if __name__ == '__main__':

    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser()
    parser.add_argument('date', default=datetime.now().strftime('%Y%m%d'), nargs='?')
    parser.add_argument('-s', '--start')
    parser.add_argument('-e', '--end', default=datetime.now().strftime('%Y%m%d'))
    args = parser.parse_args()

    if args.start:
        print get_weight(args.start, args.end)
    else:
        print get_weight(args.date, args.date)
