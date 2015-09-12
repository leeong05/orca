from ..node import (
        OperatorWithDefaultConstants,
        SameDimensionInputOutputUnaryOperator,
        NoDimensionOutputUnaryOperator,
        NoDimensionOutputBinaryOperator,
        SameDimensionOutputRightBinaryOperator,
        )
from ..dimension import Dimension

from ...operation import api


class OperatorCSWithOneConstantOneData(OperatorWithDefaultConstants):

    def __init__(self, name, default={}, **kwargs):
        OperatorWithDefaultConstants.__init__(self, name, 2, default=default, **kwargs)


class OperatorCSWithOneConstantTwoData(OperatorWithDefaultConstants):

    def __init__(self, name, default={}, **kwargs):
        OperatorWithDefaultConstants.__init__(self, name, 3, default=default, **kwargs)


class OperatorNeutralize(SameDimensionInputOutputUnaryOperator):

    def eval(self, environment, gene, date1, date2):
        df = gene.next_value(environment, date1, date2)
        return api.neutralize(df)


class OperatorRank(NoDimensionOutputUnaryOperator):

    def eval(self, environment, gene, date1, date2):
        df = gene.next_value(environment, date1, date2)
        return api.rank(df)


class OperatorPower(OperatorCSWithOneConstantOneData):

    def __init__(self, name, **kwargs):
        super(OperatorPower, self).__init__(name, {'arg1': 1}, **kwargs)

    def eval(self, environment, gene, date1, date2):
        exp = gene.next_value(environment, date1, date2)
        df = gene.next_value(environment, date1, date2)
        return api.power(df, exp)

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class OperatorGroupNeutralize(SameDimensionOutputRightBinaryOperator):

    def eval(self, environment, gene, date1, date2):
        group = gene.next_value(environment, date1, date2).iloc[-1]
        df = gene.next_value(environment, date1, date2)
        return api.group_neutralize(df, group)


class OperatorGroupRank(NoDimensionOutputBinaryOperator):

    def eval(self, environment, gene, date1, date2):
        group = gene.next_value(environment, date1, date2).iloc[-1]
        df = gene.next_value(environment, date1, date2)
        return api.group_rank(df, group)


class OperatorGroupPower(OperatorCSWithOneConstantTwoData):

    def __init__(self, name, **kwargs):
        super(OperatorGroupPower, self).__init__(name, {'arg1': 1}, **kwargs)

    def eval(self, environment, gene, date1, date2):
        exp = gene.next_value(environment, date1, date2)
        group = gene.next_value(environment, date1, date2).iloc[-1]
        df = gene.next_value(environment, date1, date2)
        return api.group_power(df, group, exp)

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()
