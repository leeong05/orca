"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import json
import cPickle
import msgpack
from collections import OrderedDict
from datetime import datetime
import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm

from orca import logger
from orca import DATES
from orca.mongo.quote import QuoteFetcher
from orca.mongo.industry import IndustryFetcher
from orca.operation import api

def read_frame(fname, ftype='csv'):
    if ftype == 'csv':
        return pd.read_csv(fname, header=0, parse_dates=[0], index_col=0)
    elif ftype == 'pickle':
        return pd.read_pickle(fname)
    elif ftype == 'msgpack':
        return pd.read_msgpack(fname)


class AlphaCombiner(object):
    """Class to combine alphas.

    :param int periods: How many days of returns as the predicted variable?
    :param float threshold: Returns greater than this threshold will be considered as positive
    :param float quantile: Returns greater in this top quantile will be considered as positive
    :param float multiplier: Weight multiplier for positive cases

    .. note::

       ``threshold`` and ``quantile`` should not be set at the same time
    """
    LOGGER_NAME = 'combiner'

    def __init__(self, periods, threshold=None, quantile=None, multiplier=1, debug_on=False):
        self.logger = logger.get_logger(AlphaCombiner.LOGGER_NAME)
        self.set_debug_mode(debug_on)
        self.periods = periods
        self.threshold = threshold
        self.quantile = quantile
        self.multiplier = multiplier
        self.quote = QuoteFetcher(datetime_index=True, reindex=True)
        self.name_alpha = OrderedDict()
        self.weight = None
        self.is_starts, self.is_ends = [], []
        self.os_start, self.os_end = None, None
        self.X, self.Y, self.W = None, None, None
        self.oX, self.oY = None, None
        self.result = None

    def set_debug_mode(self, debug_on):
        """Enable/Disable debug level message in data fetchers.
        This is enabled by default."""
        level = logging.DEBUG if debug_on else logging.INFO
        self.logger.setLevel(level)

    def dump(self, fpath, ftype='csv'):
        """Save predicted result in file."""
        with open(fpath, 'w') as file:
            if ftype == 'csv':
                self.oY.to_csv(file)
            elif ftype == 'pickle':
                self.oY.to_pickle(file)
            elif ftype == 'msgpack':
                self.oY.to_msgpack(file)
        self.logger.info('Predicted alphas is saved in %s', fpath)
        with open(fpath+'.param', 'w') as file:
            if ftype == 'csv':
                json.dump(self.result.params.to_dict(), file)
            elif ftype == 'pickle':
                cPickle.dump(self.result.params.to_dict(), file)
            elif ftype == 'msgpack':
                msgpack.dump(self.result.params.to_dict(), file)
        self.logger.info('Parameters is saved in %s', fpath+'.param')

    def add_alpha(self, name, alpha, ftype='csv'):
        """
        :param DataFrame alpha: Alpha to be added
        """
        if isinstance(alpha, str):
            alpha = read_frame(alpha, ftype)
        self.name_alpha[name] = api.format(alpha)

    def __setitem__(self, name, alpha):
        """Convenient method wrapper of :py:meth:`add_alpha`."""
        self.add_alpha(name, alpha)

    def set_weight(self, weight):
        if isinstance(weight, str):
            weight = read_frame(weight)
        W = api.format(weight)
        self.weight = W[np.isfinite(W) & (W > 0)]

    def add_isdates(self, start=None, end=None):
        self.is_starts.append(start)
        self.is_ends.append(end)

    def set_osdates(self, start=None, end=None):
        if start:
            self.os_start = start
        if end:
            self.os_end = end

    def prepare_XYW(self):
        """Prepare inputs for regression."""
        X = pd.Panel.from_dict(self.name_alpha, intersect=True)
        if self.os_start is not None:
            index = [dt for dt in X.major_axis if dt >= datetime.strptime(str(self.os_start), '%Y%m%d')]
            X = X.reindex(major_axis=index)
        if self.os_end is not None:
            index = [dt for dt in X.major_axis if dt <= datetime.strptime(str(self.os_end), '%Y%m%d')]
            X = X.reindex(major_axis=index)
        self.oX = X.to_frame(filter_observations=False).dropna(how='all').fillna(0)
        self.logger.debug('Out-of-sample cases ready')

        X = pd.Panel.from_dict(self.name_alpha, intersect=True)
        index = None
        for is_start, is_end in zip(self.is_starts, self.is_ends):
            tIndex = X.major_axis
            if is_start is not None:
                tIndex = [dt for dt in tIndex if dt >= datetime.strptime(str(self.is_start), '%Y%m%d')]
            if is_end is not None:
                tIndex = [dt for dt in tIndex if dt <= datetime.strptime(str(self.is_end), '%Y%m%d')]
            if index is None:
                index = tIndex
            else:
                index = tIndex.union(index)
        if index is not None:
            X = X.reindex(major_axis=index)

        self.logger.debug('In-sample cases ready')

        startdate, enddate = X.major_axis[0].strftime('%Y%m%d'), X.major_axis[-1].strftime('%Y%m%d')
        edi = DATES.index(enddate)
        if edi + self.periods > len(DATES)-1:
            edi = len(DATES)-1
            self.logger.warning('Some recent observations may not have dependent variables; these will be removed from regression')
        else:
            edi = edi + self.periods
        Y = self.quote.fetch('returnsN', self.periods, startdate, DATES[edi]).shift(-self.periods)
        Y = Y.ix[X.major_axis]
        if self.threshold:
            Y[Y >= self.threshold] = 0.1
            Y[Y < self.threshold] = -0.1
        if self.quantile:
            rY = Y.rank(axis=1)
            Y[rY >= self.quantile] = 0.1
            Y[rY < self.quantile] = -0.1

        if self.weight is None:
            W = pd.DataFrame(1, index=Y.index, columns=Y.columns)
            if self.threshold:
                W[Y == 1] *= self.multiplier
            self.logger.debug('No weight matrix specified, default to weight with multiplier {}'.format(self.multiplier))
        else:
            W = self.weight.ix[Y.index]

        names = self.name_alpha.keys() + ['returns', 'weight']
        tmp = {}
        for name in self.name_alpha.keys():
            tmp[name] = X[name]
        tmp['returns'], tmp['weight'] = Y, W
        XYW = pd.Panel.from_dict(tmp, intersect=True).reindex(items=names).to_frame(filter_observations=False)
        X = XYW.iloc[:, :-2].dropna(how='all')
        Y = XYW.iloc[:, -2].dropna()
        index = X.index.intersection(Y.index)
        XYW = XYW.reindex(index=index).fillna(0)

        self.X, self.Y, self.W = XYW.iloc[:,:-2], XYW.iloc[:, -2], XYW.iloc[:, -1]
        self.logger.debug('Regression model inputs ready')

    def fit(self):
        model = sm.WLS(self.Y, self.X, self.W)
        self.result = model.fit()
        self.logger.debug('Model fitting done')

    def predict(self):
        self.oY = pd.Series(self.result.predict(self.oX), index=self.oX.index)
        self.oY = api.format(self.oY.unstack()) * 242. / self.periods
        self.logger.debug('Model prediction done')

    def run(self, fpath):
        """Main interface.

        :param str fpath: Valid file path.
        """
        self.prepare_XYW()
        self.fit()
        self.logger.info('\nR^2: %f\n%s', self.result.rsquared, self.result.summary())
        self.predict()
        self.dump(fpath)


class IndustryAlphaCombiner(object):
    """Class to combine alphas industry-by-industry.

    :param int level: Which level of industry to be used for grouping? Must be one of (1, 2, 3)
    """

    fetcher = IndustryFetcher(datetime_index=True, reindex=True)
    industry = {
            'level1': None,
            'level2': None,
            'level3': None,
            }

    @classmethod
    def fetch_industry(cls, startdate, level):
        level = 'level%d' % int(level)
        if cls.industry[level] is None or startdate < cls.industry[level].index[0]:
            cls.industry[level] = cls.fetcher.fetch(level, startdate).fillna('')
        return cls.industry[level]

    def __init__(self, *args, **kwargs):
        self.level = kwargs.pop('level', 1)
        self.industries = IndustryAlphaCombiner.fetcher.fetch_info('name', level=self.level).keys()
        self.combos = {industry: AlphaCombiner(*args, **kwargs) for industry in self.industries}
        self.oY, self.params = None, {}

    def dump(self, fpath, ftype='csv', dump_all=False):
        if dump_all:
            for ind, combo in self.combos:
                combo.dump(fpath+'_'+ind, ftype=ftype)
        with open(fpath, 'w') as file:
            if ftype == 'csv':
                self.oY.to_csv(file)
            elif ftype == 'pickle':
                self.oY.to_pickle(file)
            elif ftype == 'msgpack':
                self.oY.to_msgpack(file)
        self.logger.info('Predicted alphas is saved in %s', fpath)
        with open(fpath+'.param', 'w') as file:
            if ftype == 'csv':
                json.dump(self.result.params.to_dict(), file)
            elif ftype == 'pickle':
                cPickle.dump(self.result.params.to_dict(), file)
            elif ftype == 'msgpack':
                msgpack.dump(self.result.params.to_dict(), file)
        self.logger.info('Parameters is saved in %s', fpath+'.param')

    def add_alpha(self, name, alpha, ftype='csv'):
        if isinstance(alpha, str):
            alpha = read_frame(alpha, ftype)
        alpha = api.format(alpha)
        industry = IndustryAlphaCombiner.fetch_industry(alpha.index[0], self.level).ix[alpha.index]
        for ind, combo in self.combos.iteritems():
            combo.add_alpha(alpha[industry == ind])

    def __setitem__(self, name, alpha):
        self.add_alpha(name, alpha)

    def set_weight(self, weight):
        if isinstance(weight, str):
            weight = read_frame(weight)
        weight = api.format(weight)
        industry = IndustryAlphaCombiner.fetch_industry(weight.index[0], self.level).ix[weight.index]
        for ind, combo in self.combos.iteritems():
            combo.set_weight(weight[industry == ind])

    def add_isdates(self, start=None, end=None):
        for _, combo in self.combos.iteritems():
            combo.add_isdates(start, end)

    def set_osdates(self, start=None, end=None):
        for _, combo in self.combos.iteritems():
            combo.set_osdates(start, end)

    def prepare_XYW(self):
        for _, combo in self.combos.iteritems():
            combo.prepare_XYW()

    def fit(self):
        for ind, combo in self.combos.iteritems():
            combo.fit()
            self.params[ind] = combo.result.params.to_dict()

    def predict(self):
        oYs = []
        for _, combo in self.combos.iteritems():
            combo.predict()
            oY = combo.oY.dropna(how='all', axis=1)
            oYs.append(oY)
        self.oY = api.format(pd.concat(oYs, axis=1))

    def run(self, fpath, dump_all=False):
        """Main interface.

        :param str fpath: Valid file path.
        """
        self.prepare_XYW()
        self.fit()
        self.predict()
        self.dump(fpath, dump_all)


if __name__ == '__main__':
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--ftype', help='File type', choices=('csv', 'pickle', 'msgpack'), default='csv')
    parser.add_argument('--periods', help='Days of returns', type=int, default=20)
    parser.add_argument('--threshold', help='Returns greater than this threshold considered to be positive', type=float)
    parser.add_argument('--quantile', help='Returns in this top quantile considered to be positive', type=float)
    parser.add_argument('--multiplier', help='Multiplier weight given on positive cases', type=float, default=4)
    parser.add_argument('--debug_on', help='Whether display debug log message', action='store_true')
    parser.add_argument('--is_starts', help='IS startdate', nargs='+')
    parser.add_argument('--is_ends', help='IS enddate', nargs='+')
    parser.add_argument('--os_start', help='OS startdate', type=str)
    parser.add_argument('--os_end', help='OS enddate', type=str)
    parser.add_argument('--dir', help='Input directory, each file contained is assumed to be an alpha file', type=str)
    parser.add_argument('--file', help='Input file, each row in the format: name path_to_a_csv_file', type=str)
    parser.add_argument('--dump', help='The output file name', type=str, default='combo')
    parser.add_argument('--industry', action='store_true')
    parser.add_argument('--level', choices=(1, 2, 3), default=1)
    args = parser.parse_args()

    if args.industry:
        combiner = IndustryAlphaCombiner(args.periods, args.threshold, args.quantile, args.multiplier, debug_on=args.debug_on, level=args.level)
    else:
        combiner = AlphaCombiner(args.periods, args.threshold, args.quantile, args.multiplier, debug_on=args.debug_on)

    if args.file:
        with open(args.file) as file:
            for line in file:
                name, fpath = line.split('\s+')
                if name.upper() == 'WEIGHT':
                    combiner.set_weight(fpath)
                else:
                    combiner.add_alpha(name, fpath, args.ftype)

    if args.dir:
        assert os.path.exists(args.dir)
        for name in os.listdir(args.dir):
            if name.upper() == 'WEIGHT':
                combiner.set_weight(os.path.join(args.dir, name))
            else:
                combiner.add_alpha(name, os.path.join(args.dir, name), args.ftype)

    if args.is_end and args.os_start is None:
        args.os_start = args.is_end

    for is_start, is_end in zip(args.is_starts, args.is_ends):
        combiner.add_isdates(start=is_start, end=is_end)
    combiner.set_osdates(start=args.os_start, end=args.os_end)

    combiner.run()
