from abc import ABC, abstractmethod

import pandas as pd
from lxml import etree

from ..util.datetime import shift_date
from ..operation.api import intersect

from .node import (
        Constant,
        Data,
        )
from .gene import Gene
from .dimension import Dimension


class Environment(ABC):

    @abstractmethod
    def set_dates(self):
        raise NotImplementedError

    @abstractmethod
    def add_datas(self):
        raise NotImplementedError

    @abstractmethod
    def add_operators(self):
        raise NotImplementedError

    @abstractmethod
    def set_returns(self):
        raise NotImplementedError

    def __init__(self):
        self.set_dates()
        assert 'dates' in self.__dict__

        self.add_datas()
        assert 'datas' in self.__dict__
        if not hasattr(self, 'data_pool'):
            self.data_pool = list(self.datas.keys())

        self.add_operators()
        assert 'operators' in self.__dict__
        if not hasattr(self, 'operator_pool'):
            self.operator_pool = list(self.operators.keys())

        self.set_universe()
        self.set_returns()
        assert 'universe' in self.__dict__
        assert 'returns' in self.__dict__
        assert 'ranked_returns' in self.__dict__

    def set_universe(self):
        self.universe = None

    def add_data(self, k, v):
        assert k not in self.datas
        self.datas[k] = v

    def get_data_value(self, name):
        assert name in self.datas
        return self.datas[name]['value']

    def get_data_dimension(self, name):
        assert name in self.datas
        return Dimension.parse(self.datas[name].get('dimension', ''))

    def add_pool_data(self, name, times=1):
        assert name in self.datas
        self.data_pool += [name] * times

    def add_operator(self, k, v):
        assert k not in self.operators
        self.operators[k] = v

    def add_pool_operator(self, name, times=1):
        assert name in self.operators
        self.operator_pool += [name] * times

    def shift_date(self, date, n, direction=None):
        return shift_date(self.dates, date, n, direction=direction)

    def get_frame(self, df, date1, date2):
        if date1 < df.index[0] or date2 > df.index[-1]:
            raise IndexError
        return df.ix[date1: date2]

    def get_returns(self, n=1):
        returns = pd.rolling_apply(self.returns, n, lambda x: (1+x).cumprod()[-1]-1.)
        returns = intersect(returns, self.universe)
        return returns, returns.rank(axis=1)

    def create_node(self, name):
        try:
            return Constant(name)
        except ValueError:
            pass

        try:
            data_dict = self.datas[name]
            return Data(name, data_dict['value'], data_dict.get('dimension', ''))
        except KeyError:
            pass

        operator_dict = self.operators[name]
        operator = operator_dict['operator']
        return operator(name, **operator_dict)

    def parse_string(self, string):
        string = string.replace('(', ' ').replace(')', ' ').replace(',', ' ')
        sequence = []
        for name in string.strip().split():
            node = self.create_node(name)
            sequence.append(node)
        return Gene(sequence)

    def parse_xmlstring(self, xmlstring):
        xml = etree.XML(xmlstring)
        sequence = []
        for elem in xml.iter():
            node = self.create_node(elem.attrib['value'])
            sequence.append(node)
        return Gene(sequence)
