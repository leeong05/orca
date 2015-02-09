"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
datefmt = DateFormatter('%Y%m%d')
from matplotlib.backends.backend_pdf import PdfPages

from orca.mongo.industry import IndustryFetcher
from orca.mongo.index import IndexQuoteFetcher
from orca.utils import dateutil
from orca.utils.io import read_frame
from orca.operation import api


class Weight(object):
    """Class to analyse portfolio weight decomposition through time."""

    def __init__(self, alpha, n):
        self.alpha = api.format(alpha)
        self.alpha = self.alpha.rank(axis=1, ascending=False)
        self.rank_alpha = self.alpha[self.alpha <= n]
        self.alpha = n+1 - self.rank_alpha
        self.alpha = api.scale(self.alpha)
        self.dates = dateutil.to_datestr(self.alpha.index)
        self.index_fetcher = IndexQuoteFetcher(datetime_index=True)
        industry_fetcher = IndustryFetcher(datetime_index=True, reindex=True)
        self.industry = industry_fetcher.fetch_window('level1', self.dates).fillna('')
        self.industry_weight = {}
        for ind in industry_fetcher.fetch_info('name', level=1).keys():
            indweight = self.alpha[self.industry == ind].sum(axis=1)
            self.industry_weight[ind] = indweight
        self.industry_weight = pd.DataFrame(self.industry_weight)
        self.index_data = {}

    def overlap(self, index='HS300', dname='close'):
        data = self.index_fetcher.fetch_window(dname, self.dates, index=index)
        data.name = index+'_'+dname
        self.index_data[index+'_'+dname] = data

    def plot(self, industry=[], name='', index_data='', startdate=None, enddate=None, delay=1, sumweight=True):
        dates = self.dates[:]
        if startdate:
            dates = [date for date in dates if date >= startdate]
        if enddate:
            dates = [date for date in dates if date <= enddate]
        dates = pd.to_datetime(dates)
        indweight = self.industry_weight[industry].shift(delay).ix[dates]
        if sumweight:
            indweight = indweight.sum(axis=1)
            if name:
                indweight.name = name
            indweight = pd.DataFrame({indweight.name: indweight})
        x, y1 = dates[delay:], indweight.iloc[delay:]

        fig, ax1 = plt.subplots()
        for name, y in y1.iteritems():
            ax1.plot(x, y, label=name)
        ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=3, fancybox=True, shadow=True)
        ax1.format_xdata = datefmt
        ax1.xaxis.set_major_formatter(datefmt)

        if index_data:
            if len(self.index_data) == 1:
                index = self.index_data[self.index_data.keys()[0]]
            else:
                index = self.index_data[index_data]
            index = index.ix[dates]
            y2 = index.iloc[delay:]
            ax2 = ax1.twinx()
            ax2.plot(x, y2, 'r')
            ax2.format_xdata = datefmt
            ax2.xaxis.set_major_formatter(datefmt)
            ax2.set_ylabel(y2.name)

        fig.autofmt_xdate()
        return fig


if __name__ == '__main__':
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'))
    parser.add_argument('--ntop', type=int, required=True)
    parser.add_argument('-i', '--index', type=str, help='Name of the index')
    parser.add_argument('--dname', type=str, help='Name of the index data', default='close')
    parser.add_argument('--industry', help='List of industries', nargs='+')
    parser.add_argument('--name', help='Name of the summed up industries', type=str)
    parser.add_argument('--sum', help='Whether to sum up these industries weight', action='store_true')
    parser.add_argument('-s', '--start', help='IS startdate', nargs='*')
    parser.add_argument('-e', '--end', help='IS enddate', nargs='*')
    parser.add_argument('--delay', help='Delay of index data w.r.t. weight', default=1, type=int)
    parser.add_argument('--pdf', action='store_true', help='Whether to save plots in a PDF file')
    args = parser.parse_args()

    if args.start:
        if len(args.start) == 1:
            args.end = [None]
        assert args.end and len(args.start) == len(args.end)
    else:
        args.start, args.end = [None], [None]

    alpha = read_frame(args.alpha, args.ftype)
    weight = Weight(alpha, args.ntop)
    if args.index:
        weight.overlap(index=args.index, dname=args.dname)

    _industry = args.industry[:]
    args.industry = []
    args.industries = {}
    for industry in _industry:
        if not os.path.isfile(industry):
            args.industry.append(industry)
            last = weight.industry.iloc[-1]
            args.industries[industry] = list(last[last == industry].index)
            continue
        industries = {}
        with open(industry) as file:
            for line in file:
                try:
                    sid, ind = line.strip().split()
                    assert len(sid) == 6 and sid[:2] in ('00', '30', '60')
                    if ind not in industries:
                        industries[ind] = set()
                    industries[ind].add(sid)
                except:
                    pass
        args.industry = []
        for ind, sids in industries.iteritems():
            weight.industry_weight[ind] = weight.alpha.ix[:, list(sids)].sum(axis=1)
            args.industry.append(ind)
            args.industries[ind] = list(sids)

    if not args.name:
        if len(args.industry) == 1:
            args.name = args.industry[0]
        elif args.sum:
            parser.print_help()
            exit(0)

    figs = []
    for start, end in zip(args.start, args.end):
        fig = weight.plot(
                industry=args.industry,
                name=args.name,
                index_data=args.index+'_'+args.dname if args.index else None,
                startdate=start,
                enddate=end,
                delay=args.delay,
                sumweight=args.sum)
        figs.append(fig)

    if args.pdf:
        pdf = os.path.basename(args.alpha)+'.pdf'
        if os.path.exists(pdf):
            os.remove(pdf)
        pp = PdfPages(pdf)
        for fig in figs:
            pp.savefig(fig)
        pp.close()
    else:
        plt.show()

    last_alpha = weight.rank_alpha.iloc[-1]
    for industry, sids in args.industries.iteritems():
        print industry
        alpha = last_alpha.ix[sids].dropna()
        alpha.sort()
        print alpha

