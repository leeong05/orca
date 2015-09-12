from fractions import Fraction

import numpy as np

from ..node import (
        SameDimensionInputOutputUnaryOperator,
        BinaryOperator,
        SameDimensionInputOutputBinaryOperator,
        )


class OperatorAdd(SameDimensionInputOutputBinaryOperator):

    def eval(self, environment, gene, date1, date2):
        return gene.next_value(environment, date1, date2) + gene.next_value(environment, date1, date2)


class OperatorSubtract(SameDimensionInputOutputBinaryOperator):

    def eval(self, environment, gene, date1, date2):
        return gene.next_value(environment, date1, date2) - gene.next_value(environment, date1, date2)

OperatorSub = OperatorSubtract


class OperatorMultiply(BinaryOperator):

    def eval(self, environment, gene, date1, date2):
        return gene.next_value(environment, date1, date2) * gene.next_value(environment, date1, date2)

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[0].dimension * children[1].dimension

OperatorMul = OperatorMultiply


class OperatorDivide(BinaryOperator):

    def eval(self, environment, gene, date1, date2):
        return gene.next_value(environment, date1, date2) / gene.next_value(environment, date1, date2)

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[0].dimension / children[1].dimension

OperatorDiv = OperatorDivide


class OperatorExp(BinaryOperator):

    def eval(self, environment, gene, date1, date2):
        exp = gene.next_value(environment, date1, date2)
        df = gene.next_value(environment, date1, date2)
        return df ** exp

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[1].dimension ** Fraction(str(children[0]))


class OperatorSignedExp(BinaryOperator):

    def eval(self, environment, gene, date1, date2):
        exp = gene.next_value(environment, date1, date2)
        df = gene.next_value(environment, date1, date2)
        return np.sign(df) * (df.abs()) ** exp

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[1].dimension ** Fraction(str(children[0]))


class OperatorAbs(SameDimensionInputOutputUnaryOperator):

    def eval(self, environment, gene, date1, date2):
        df = gene.next_value(environment, date1, date2)
        return df.abs()
