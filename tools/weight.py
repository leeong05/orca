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
from orca.operation import api
from orca.utils import dateutil

def read_frame(fname, ftype='csv'):
    if ftype == 'csv':
        return pd.read_csv(fname, header=0, parse_dates=[0], index_col=0)
    elif ftype == 'pickle':
        return pd.read_pickle(fname)
    elif ftype == 'msgpack':
        return pd.read_msgpack(fname)


class Weight(object):
    """Class to analyse portfolio weight decomposition through time."""

    def __init__(self, alpha):
        self.alpha = api.format(read_frame(alpha))
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

    def plot(self, industry=[], name='', index_data='', startdate=None, enddate=None, delay=1):
        dates = self.dates[:]
        if startdate:
            dates = [date for date in dates if date >= startdate]
        if enddate:
            dates = [date for date in dates if date <= enddate]
        dates = pd.to_datetime(dates)
        series = self.industry_weight[industry].sum(axis=1).shift(delay).ix[dates]
        if name:
            series.name = name
        if len(self.index_data) == 1:
            index = self.index_data[self.index_data.keys()[0]]
        else:
            index = self.index_data[index_data]
        index = index.ix[dates]
        x, y1, y2 = dates[delay:], series.iloc[delay:], index.iloc[delay:]
        corr = y1.corr(y2)

        fig, ax1 = plt.subplots()
        ax1.plot(x, y1)
        ax1.format_xdata = datefmt
        ax1.xaxis.set_major_formatter(datefmt)
        ax1.set_ylabel(y1.name)
        ax1.set_title('Correlation: %.2f' % corr)

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
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'), default='csv')
    parser.add_argument('-i', '--index', type=str, help='Name of the index', default='HS300')
    parser.add_argument('--dname', type=str, help='Name of the index data', default='close')
    parser.add_argument('--industry', help='List of industries to be summed up', default='HS300', nargs='+')
    parser.add_argument('--name', help='Name of the summed up industries', type=str)
    parser.add_argument('--starts', help='IS startdate', nargs='*')
    parser.add_argument('--ends', help='IS enddate', nargs='*')
    parser.add_argument('--delay', help='Delay of index data w.r.t. weight', default=1, type=int)
    parser.add_argument('--pdf', action='store_true', help='Whether to save plots in a PDF file')
    args = parser.parse_args()

    if args.starts:
        if len(args.starts) == 1:
            args.ends = [None]
        assert args.ends and len(args.starts) == len(args.ends)
    else:
        args.starts, args.ends = [None], [None]

    weight = Weight(args.alpha)
    weight.overlap(index=args.index, dname=args.dname)
    if not args.name:
        if len(args.industry) == 1:
            args.name = args.industry[0]
    if not args.name:
        print 'Please provide name for the industries'
        parser.print_usage()
        exit(0)


    figs = []
    for start, end in zip(args.starts, args.ends):
        fig = weight.plot(
                industry=args.industry,
                name=args.name,
                index_data=args.index+'_'+args.dname,
                startdate=start,
                enddate=end,
                delay=args.delay)
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
