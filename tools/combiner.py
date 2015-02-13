"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import re
from collections import OrderedDict
import abc
import argparse

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

    def __init__(self, combiner, gcombiner=None, options={}):
        self.combiner = combiner
        self.gcombiner = combiner if gcombiner is None else gcombiner
        self.options = options

    def run(self):
        self.parse_args(self)
        self.parse_options(self)
        self.parse_file(self)
        X = self.group_fit(self)
        dump_frame(X, self.args.output, self.args.type)

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('file', help='Each line in the input file should have three columns: name, groupid, file_path; separated by space or comma. Line starting with # is ommitted')
        parser.add_argument('-p', '--periods', help='Returns period', type=int, default=10)
        parser.add_argument('-s', '--start', help='IS startdate', type=str)
        parser.add_argument('-e', '--end', help='IS enddate', type=str)
        parser.add_argument('-o', '--output', help='Output file name; overwritten if exists', type=str, required=True)
        parser.add_argument('-t', '--type', choices=('csv', 'msgpack', 'pickle'), help='Output file type', default='msgpack', type=str)
        for opt in self.options:
            parser.add_argument('--'+opt, type=str)

        args = parser.parse_args()
        self.args = args

    def parse_options(self):
        for opt in self.options:
            self.options[opt] = self.args.__dict__[opt]

    def parse_file(self):
        self.groups = {}
        pattern = re.compile(r'[#\s]')
        with open(self.args.file) as file:
            for line in file:
                if line.startswith('#'):
                    continue
                cont = re.split(pattern, line.strip())
                if len(cont) != 3:
                    continue
                name, group, fpath = cont
                if group not in self.groups:
                    self.groups[group] = self.combiner(self.args.periods, self.options)
                self.groups[group].add_alpha(name, fpath)

    def group_fit(self):
        self.fitted = {}
        for group, combiner in self.groups.iteritems():
            combiner.prepare_data()
            X, Y = combiner.get_XY(self.args.start, self.args.end)
            self.fitted[group] = combiner.fit(X, Y)

        if len(self.fitted) == 1:
            return self.fitted.values()[0]

        gcombiner = self.gcombiner(self.args.periods, self.options)
        for group, X in self.fitted.iteritems():
            gcombiner.add_alpha(group, X)
        gcombiner.prepare_data()
        X, Y = gcombiner.get_XY(self.args.start, self.args.end)
        return gcombiner.fit(X, Y)
