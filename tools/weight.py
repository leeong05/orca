"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
datefmt = DateFormatter('%Y%m%d')
from matplotlib.backends.backend_pdf import PdfPages

import magic

from orca.mongo.industry import IndustryFetcher
from orca.mongo.index import IndexQuoteFetcher
from orca.mongo.components import ComponentsFetcher
from orca.mongo.sywgquote import SYWGQuoteFetcher

industry_fetcher = IndustryFetcher(datetime_index=True, reindex=True)
indexquote_fetcher = IndexQuoteFetcher(datetime_index=True)
components_fetcher = ComponentsFetcher(datetime_index=True)
sywgquote_fetcher = SYWGQuoteFetcher(datetime_index=True, use_industry=True)

from orca.utils import dateutil
from orca.utils.io import read_frame
from orca.operation import api


class Weight(object):
    """Class to analyse portfolio weight decomposition through time."""

    def __init__(self, alpha, n, rank=None):
        self.alpha = api.format(alpha)
        self.alpha = self.alpha.rank(axis=1, ascending=False)
        self.rank_alpha = self.alpha[self.alpha <= n]
        if rank is not None:
            self.alpha = rank - np.floor(self.rank_alpha/(n+1)*rank)
        self.alpha = api.scale(self.alpha)
        self.dates = dateutil.to_datestr(self.alpha.index)

    def get_industry_weight(self, industry=[]):
        sid_ind = industry_fetcher.fetch_window('level1', self.dates).fillna('')
        ind_wgt = {}
        for ind in industry:
            ind_alpha = self.alpha[sid_ind == ind]
            wgt = ind_alpha.sum(axis=1)
            ind_wgt[ind] = wgt
        return pd.DataFrame(ind_wgt)

    def get_component_weight(self, component=[]):
        comp_wgt = {}
        for comp in component:
            if comp == 'CYB':
                cyb = [sid.startswith('30') for sid in self.alpha.columns]
                cyb_alpha = self.alpha.ix[:, cyb]
                wgt = cyb_alpha.sum(axis=1)
                comp_wgt[comp] = wgt
            elif comp == 'ZXB':
                zxb = [sid.startswith('002') for sid in self.alpha.columns]
                zxb_alpha = self.alpha.ix[:, zxb]
                wgt = zxb_alpha.sum(axis=1)
                comp_wgt[comp] = wgt
            else:
                try:
                    c = components_fetcher.fetch_window(comp, self.dates, as_bool=True).astype(float)
                    comp_alpha = self.alpha.ix[:, c.columns] * c
                    wgt = comp_alpha.sum(axis=1)
                    comp_wgt[comp] = wgt
                except:
                    pass
        return pd.DataFrame(comp_wgt)

    def get_index_quote(self, index='HS300', shift=0):
        ser = indexquote_fetcher.fetch_window('close', self.dates, index=index).shift(shift)
        ser.name = index
        return ser

    def plot(self, df, index_quote=None):
        fig, ax1 = plt.subplots()
        for label, y in df.iteritems():
            ax1.plot(self.alpha.index, y, label=label)
        ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=3, fancybox=True, shadow=True)
        ax1.format_xdata = datefmt
        ax1.xaxis.set_major_formatter(datefmt)

        if index_quote is not None:
            ax2 = ax1.twinx()
            ax2.plot(self.alpha.index, index_quote, 'r')
            ax2.format_xdata = datefmt
            ax2.xaxis.set_major_formatter(datefmt)
            ax2.set_ylabel(index_quote.name)

        fig.autofmt_xdate()
        return fig


if __name__ == '__main__':
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha', help='Alpha file')
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'))
    parser.add_argument('-n', '--ntop', help='Number of stocks used in analysis', type=int, required=True)
    parser.add_argument('-r', '--rank', help='Weight these stocks by ranks', type=int)
    parser.add_argument('--index', help='Name of the index', type=str)
    parser.add_argument('-i', '--industry', help='List of SYWG Level1 industries', nargs='*')
    parser.add_argument('-c', '--component', nargs='*', help='List of groupings to analyse its components; for example: HS300, CS500, ZXB, CYB etc')
    parser.add_argument('--sum_i', help='Whether to sum up these industries weight', action='store_true')
    parser.add_argument('--sum_c', help='Whether to sum up these components weight', action='store_true')
    parser.add_argument('-s', '--start', help='Startdate', type=str)
    parser.add_argument('-e', '--end', help='Enddate', type=str)
    parser.add_argument('--shift', help='Shift of index data w.r.t. weight', default=0, type=int)
    parser.add_argument('--pdf', type=str, help='PDF file to save the plot')
    args = parser.parse_args()

    alpha = read_frame(args.alpha, args.ftype)

    if args.pdf and os.path.exists(args.pdf):
        with magic.Magic() as m:
            ftype = m.id_filename(args.pdf)
            if ftype[:3] != 'PDF':
                print 'The argument --pdf if exists must be a PDF file'
                exit(0)
    if args.start:
        alpha = alpha.ix[args.start:]
    if args.end:
        alpha = alpha.ix[:args.end]
    weight = Weight(alpha, args.ntop, args.rank)

    if args.industry:
        ind_wgt = weight.get_industry_weight(args.industry)
        if args.sum_i and len(args.industry) > 1:
            colname = 'Sum of ' + ', '.join(list(ind_wgt.columns))
            ind_wgt = pd.DataFrame({colname: ind_wgt.sum(axis=1)})
    else:
        ind_wgt = None

    if args.component:
        comp_wgt = weight.get_component_weight(args.component)
        if args.sum_c and len(args.component) > 1:
            colname = 'Sum of ' + ', '.join(list(comp_wgt.columns))
            comp_wgt = pd.DataFrame({colname: comp_wgt.sum(axis=1)})
    else:
        comp_wgt = None

    if ind_wgt is None and comp_wgt is None:
        print 'Either --industry or --component(-c) must not be empty'
        parser.print_help()
        exit(0)
    else:
        if ind_wgt is None:
            df = comp_wgt
        else:
            if comp_wgt is None:
                df = ind_wgt
            else:
                df = pd.concat([ind_wgt, comp_wgt], axis=1)

    if args.index:
        index_quote = weight.get_index_quote(args.index, args.shift)
    else:
        index_quote = None

    fig = weight.plot(df, index_quote)

    if args.pdf:
        pp = PdfPages(args.pdf)
        pp.savefig(fig)
        pp.close()
        print 'Saved figure in', args.pdf
    else:
        plt.show()
