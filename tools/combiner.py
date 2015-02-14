"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from collections import OrderedDict
import abc
import argparse
import imp

import numpy as np
import pandas as pd

from orca.mongo.quote import QuoteFetcher
from orca.utils.io import (
        read_frame,
        dump_frame,
        )


class AlphaCombinerBase(object):
    """Base class to combine alphas.

    :param int periods: How many days of returns as the predicted variable?

    .. note::

       This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, periods, **kwargs):
        self.periods = periods
        self.quote = QuoteFetcher(datetime_index=True, reindex=True)
        self.name_alpha = OrderedDict()
        self.data = None
        self.__dict__.update(kwargs)

    def add_alpha(self, name, alpha, ftype=None):
        """
        :param DataFrame alpha: Alpha to be added
        """
        if isinstance(alpha, str):
            alpha = read_frame(alpha, ftype=ftype)
        elif isinstance(alpha, pd.DataFrame):
            alpha.index = pd.to_datetime(alpha.index)
        self.name_alpha[name] = alpha.stack()

    def __setitem__(self, name, alpha):
        """Convenient method wrapper of :py:meth:`add_alpha`."""
        self.add_alpha(name, alpha)

    def prepare_data(self):
        """Prepare inputs for regression."""
        X = pd.DataFrame(self.name_alpha)
        X.index.names = ['date', 'sid']
        X = X.dropna(axis=0, how='all')
        self.data = X.reset_index()
        self.data.index = X.index

        startdate, enddate = self.data['date'].min().strftime('%Y%m%d'), self.data['date'].max().strftime('%Y%m%d')
        Y = self.quote.fetch('returnsN', self.periods, startdate, enddate, self.periods)[np.unique(self.data['sid'])]
        Y = Y.shift(-self.periods).iloc[self.periods:].stack()
        Y = Y.ix[X.index]
        self.data['returns'] = Y

        self.data = self.data.ix[Y.notnull()]

    def get_XY(self, start=None, end=None):
        if start is not None:
            data = self.data.query('date >= {!r}'.format(str(start)))
        if end is not None:
            data = data.query('data <= {!r}'.format(str(end)))
        return data.iloc[:, 2:-1], data.iloc[:, -1]

    @abc.abstractmethod
    def fit(self, X, Y):
        raise NotImplementedError


class CombinerWrapper(object):

    def run(self):
        self.parse_args(self)
        self.fit_by_group(self)
        X = self.combine_groups(self)
        dump_frame(X, self.output, self.filetype)

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('file', help='A Python configuration script file')
        parser.add_argument('-s', '--start', help='IS startdate', type=str)
        parser.add_argument('-e', '--end', help='IS enddate', type=str)
        parser.add_argument('-o', '--output', help='Output file name; overwritten if exists', type=str, required=True)
        parser.add_argument('-t', '--type', choices=('csv', 'msgpack', 'pickle'), help='Output file type', default='msgpack', type=str)
        args = parser.parse_args()

        self.start, self.end = args.start, args.end

        mod = imp.load_source(args.file)
        self.groups = {}
        for group, config in mod.groups.iteritems():
            combiner = config['combiner']
            for name, alpha_config in config['alphas'].iteritems():
                alpha = read_frame(alpha_config['path'], alpha_config.get('filetype', None))
                combiner.add_alpha(name, alpha)
            self.groups[group] = combiner

        if 'group_combiner' not in mod.__dict__:
            try:
                assert len(self.groups) == 1
            except:
                print 'For multiple groups, you should specify "group_combiner" in', args.file
                raise
            self.group_combiner = None
        else:
            if len(self.groups) == 1:
                print 'With only one group specified, the "group_combiner" is not used'
                self.group_combiner = None
            else:
                self.group_combiner = mod.group_combiner['combiner']
                groups = mod.group_combiner['groups']
                if not groups:
                    self.groups_to_combine = self.groups.keys()
                else:
                    self.groups_to_combine = [group for group in groups if group in self.groups.keys()]
                self.output = mod.group_combiner['output']
                self.filetype = mod.group_combiner.get('filetype', 'msgpack')

    def fit_by_group(self):
        for group, combiner in self.groups.iteritems():
            combiner.prepare_data()
            X, Y = combiner.get_XY(self.start, self.end)
            self.groups[group] = combiner.fit(X, Y)

    def combine_groups(self):
        if self.group_combiner is None:
            return self.groups.values()[0]

        for group, X in self.groups.iteritems():
            self.group_combiner.add_alpha(group, X)
        self.group_combiner.prepare_data()
        X, Y = self.group_combiner.get_XY(self.start, self.end)
        return self.group_combiner.fit(X, Y)
