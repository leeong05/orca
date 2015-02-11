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
from orca.mongo.kday import UnivFetcher

industry_fetcher = IndustryFetcher(datetime_index=True, reindex=True)
indexquote_fetcher = IndexQuoteFetcher(datetime_index=True)
components_fetcher = ComponentsFetcher(datetime_index=True)
sywgquote_fetcher = SYWGQuoteFetcher(datetime_index=True, use_industry=True)
univ_fetcher = UnivFetcher(datetime_index=True, reindex=True)

from orca.utils import dateutil
from orca.utils.io import read_frame
from orca.operation import api


class Weight(object):
    """Class to analyse portfolio weight decomposition through time."""

    def __init__(self, alpha, n, rank=None):
        self.alpha = api.format(alpha)
        self.rank_alpha = self.alpha.rank(axis=1, ascending=False)
        self.rank_alpha = self.rank_alpha[self.rank_alpha <= n]
        if rank is None:
            self.alpha = (self.rank_alpha <= n).astype(float)
        else:
            if rank < 0:
                self.alpha = self.alpha[self.rank_alpha <= n]
            else:
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

    def get_universe_weight(self, universe=[]):
        univ_wgt = {}
        for univ in universe:
            u = univ_fetcher.fetch_window(univ, self.dates)
            univ_alpha = self.alpha * u.astype(float)
            wgt = univ_alpha.sum(axis=1)
            univ_wgt[univ] = wgt
        return pd.DataFrame(univ_wgt)

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

    def plot(self, df, index_quote=None, ylim=None):
        fig, ax1 = plt.subplots()
        for label, y in df.iteritems():
            ax1.plot(self.alpha.index, y, label=label)
        if ylim is not None:
            ax1.set_ylim(ylim)
        ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=3, fancybox=True, shadow=True)
        ax1.format_xdata = datefmt
        ax1.xaxis.set_major_formatter(datefmt)

        if index_quote is not None:
            ax2 = ax1.twinx()
            index_quote = index_quote.reindex(index=self.alpha.index)
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
    parser.add_argument('-u', '--univ', nargs='*', help='List of universes to analyse its components; for example:TotalCap70Q  etc')
    parser.add_argument('--sum_i', help='Whether to sum up these industries weight', action='store_true')
    parser.add_argument('--sum_c', help='Whether to sum up these components weight', action='store_true')
    parser.add_argument('--sum_u', help='Whether to sum up these universes weight', action='store_true')
    parser.add_argument('-s', '--start', help='Startdate', type=str)
    parser.add_argument('-e', '--end', help='Enddate', type=str)
    parser.add_argument('--shift', help='Shift of index data w.r.t. weight', default=0, type=int)
    parser.add_argument('--pdf', type=str, help='PDF file to save the plot')
    parser.add_argument('--png', type=str, help='PNG file to save the plot')
    parser.add_argument('--ylim', nargs=2, help='Y-axis limit')
    args = parser.parse_args()

    alpha = read_frame(args.alpha, args.ftype)

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

    if args.univ:
        univ_wgt = weight.get_universe_weight(args.univ)
        if args.sum_u and len(args.univ) > 1:
            colname = 'Sum of ' + ', '.join(list(univ_wgt.columns))
            univ_wgt = pd.DataFrame({colname: univ_wgt.sum(axis=1)})
    else:
        univ_wgt = None

    dfs = []
    if ind_wgt is not None:
        dfs.append(ind_wgt)
    if comp_wgt is not None:
        dfs.append(comp_wgt)
    if univ_wgt is not None:
        dfs.append(univ_wgt)
    if len(dfs) == 0:
        print 'At least one of --industry(-i), --component(-c), --univ(-u) must not be empty'
        parser.print_help()
        exit(0)
    df = pd.concat(dfs, axis=1)

    if args.index:
        index_quote = weight.get_index_quote(args.index, args.shift)
    else:
        index_quote = None

    if args.ylim:
        args.ylim = [float(y) for y in args.ylim]
    fig = weight.plot(df, index_quote, args.ylim)

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
