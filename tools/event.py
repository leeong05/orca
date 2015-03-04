"""
.. moduleauthor:: Li, Wang<wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import magic

from orca.mongo.quote import QuoteFetcher
quote = QuoteFetcher(datetime_index=True, reindex=True)

from orca import DATES
from orca.utils.io import read_frame
from orca.utils import dateutil
from orca.operation import api


class Event(object):

    def __init__(self, alpha, rshifts, lshifts):
        self.alpha = self.rebase_index(alpha)
        unstacked = self.alpha.unstack()
        self.index = unstacked[unstacked].index
        self.rshifts, self.lshifts = rshifts, lshifts
        self.returns = self.fetch_returns()

    @staticmethod
    def rebase_index(alpha):
        res = {}
        for date in np.unique(dateutil.to_datestr(alpha.index)):
            sdf = alpha.ix[date]
            date = dateutil.find_le(DATES, date)[1]
            if isinstance(sdf, pd.DataFrame):
                res[date] = sdf.astype(int).max().astype(bool)
            else:
                res[date] = sdf
        res = pd.DataFrame(res).T.sort_index()
        res.index = pd.to_datetime(res.index)
        return res

    def fetch_returns(self):
        rshift, lshift = max(self.rshifts), max(self.lshifts)
        dates = dateutil.to_datestr(self.alpha.index)
        ei = DATES.index(dates[-1])
        edate = DATES[min(len(DATES)-2, ei+rshift)]
        ret = quote.fetch('returns', dates[0], edate, backdays=rshift+lshift)
        return ret

    @staticmethod
    def neutral(ret):
        return api.industry_neut(ret, 'level1', simple=True)

    def get_returns(self, rshift, lshift):
        ret = pd.rolling_sum(self.returns.fillna(0), rshift+lshift+1)
        ret[self.returns.isnull()] = np.nan
        return ret.shift(-rshift)

    def _get_response(self, rshift, lshift, transform=None):
        ret = self.get_returns(rshift, lshift, transform)
        if transform is not None:
            ret = transform(ret)
        return ret.unstack().ix[self.index]

    def get_response(self, mode='neutral'):
        if mode == 'neutral':
            transform = self.neutral
        else:
            transform = None

        cols, res = [], []
        for rshift, lshift in zip(self.rshifts, self.lshifts):
            col = '{}_{}'.format(lshift, rshift)
            cols.append(col)
            res.append(self._get_response(rshift, lshift, transform))
        df = pd.concat(res, axis=1)
        df.columns = cols
        return df


if __name__ == '__main__':
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'))
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('-l', '--lshift', nargs='*', type=int)
    parser.add_argument('-r', '--rshift', nargs='*', type=int)
    parser.add_argument('-m', '--mode', choices=('raw', 'neutral'), default='neutral')
    parser.add_argument('--pdf', type=str, help='PDF file to save the plot')
    parser.add_argument('--png', type=str, help='PNG file to save the plot')
    args = parser.parse_args()

    alpha = read_frame(args.alpha, args.ftype)
    alpha = alpha.notnull()
    if args.start:
        alpha = alpha.ix[args.start:]
    if args.end:
        alpha = alpha.ix[:args.end]

    if args.pdf and os.path.exists(args.pdf):
        with magic.Magic() as m:
            ftype = m.id_filename(args.pdf)
            if ftype[:3] != 'PDF':
                print 'The argument --pdf if exists must be a PDF file'
                exit(0)
    if args.png and os.path.exists(args.png):
        with magic.Magic() as m:
            ftype = m.id_filename(args.png)
            if ftype[:3] != 'PNG':
                print 'The argument --png if exists must be a PNG file'
                exit(0)

    assert args.lshift or args.rshift
    if not args.lshift:
        args.lshift = [0] * len(args.rshift)
    if not args.rshift:
        args.rshift = [0] * len(args.lshift)
    if len(args.lshift) == 1 and len(args.rshift) > 1:
        args.lshift = args.lshift * len(args.rshift)
    if len(args.rshift) == 1 and len(args.lshift) > 1:
        args.rshift = args.rshift * len(args.lshift)
    assert len(args.lshift) == len(args.rshift)

    event = Event(alpha, args.rshift, args.lshift)
    df = event.get_response(args.mode)
    res = df.mean()
    df = pd.DataFrame({
        'mean': df.mean(),
        'std': df.std(),
        'ratio': df.mean()/df.std(),
        })
    df = df.reindex(columns=['mean', 'std', 'ratio'])
    print df

    fig, ax = plt.subplots()
    ax.plot(range(len(res.index)), res.values)
    ax.set_ylabel('returns')
    ax.set_xticks(range(len(res.index)))
    ax.set_xticklabels(res.index)
    if args.pdf:
        pp = PdfPages(args.pdf)
        pp.savefig(fig)
        pp.close()
        print 'Saved figure in', args.pdf
    elif args.png:
        fig.savefig(args.png)
        print 'Saved figure in', args.png
    else:
        plt.show()
