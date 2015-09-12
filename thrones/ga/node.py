from fractions import Fraction
from abc import ABC, abstractmethod, abstractproperty

from lxml import etree

from .dimension import Dimension


class Node(ABC):

    @abstractmethod
    def eval(self, environment, gene, date1, date2):
        raise NotImplementedError

    @abstractmethod
    def validate(self, gene):
        raise NotImplementedError

    @abstractmethod
    def __str__(self):
        return ''

    @abstractproperty
    def xml(self):
        raise NotImplementedError


class Constant(Node):

    children = []

    def __init__(self, value):
        self.__value = Fraction(value)
        self.dimension = Dimension()

    def eval(self, environment, gene, date1, date2):
        v = self.__value
        if int(v) == v:
            v = int(v)
        elif float(v) == v:
            v = float(v)
        return v

    def validate(self, gene):
        pass

    def __str__(self):
        return str(self.__value)

    @property
    def xml(self):
        try:
            return self.__x
        except AttributeError:
            self.__x = etree.XML('<Constant value="%s"/>' % str(self))
            return self.__x


class Data(Node):

    children = []

    def __init__(self, name, value, dimension=''):
        self.__name = name
        self.__value = value
        self.dimension = Dimension.parse(dimension)

    def eval(self, environment, gene, date1, date2):
        return environment.get_frame(self.__value, date1, date2)

    def validate(self, gene):
        pass

    def __str__(self):
        return self.__name

    @property
    def xml(self):
        try:
            return self.__x
        except AttributeError:
            self.__x = etree.XML('<Data value="%s"/>' % str(self))
            return self.__x


class Operator(Node):

    def __init__(self, name, nchildren, **kwargs):
        self.__name = name
        self.children = [None] * nchildren

        for k, v in kwargs.items():
            if k.startswith('arg') and v != None and v != '*':
                i = int(k[3:])-1
                if 'value' in v:
                    v_value = v['value']
                    if v_value == '*':
                        del v['value']
                    elif isinstance(v_value, str):
                        v['value'] = [v_value]
                    else:
                        try:
                            v_value = list(v_value)
                        except TypeError:
                            v_value = str(v_value)
                        v['value'] = [str(i) for i in v_value]

                self.children[i] = v

    def validate_children(self, gene):
        result = []
        for i, cond in enumerate(self.children):
            child_i = gene.next_node()
            child_i.validate(gene)

            if cond:
                if 'dimension' in cond:
                    assert child_i.dimension == cond.dimension
                if 'value' in cond:
                    cond_value = cond['value']
                    str_i = str(child_i)
                    try:
                        fraction_i = Fraction(str_i)
                        assert fraction_i in [Fraction(j) for j in cond_value]
                    except ValueError:
                        assert str_i in cond_value
            result.append(child_i)
        return result

    def __str__(self):
        return self.__name

    @property
    def xml(self):
        try:
            return self.__x
        except AttributeError:
            self.__x = etree.XML('<Operator value="%s"/>' % str(self))
            return self.__x


class OperatorWithDefaultConstants(Operator):

    def __init__(self, name, nchildren, default={}, **kwargs):
        super(OperatorWithDefaultConstants, self).__init__(name, nchildren, **kwargs)
        for k, v in default.items():
            if k.startswith('arg'):
                i = int(k[3:])-1
                if self.children[i] is None:
                    if not isinstance(v, dict):
                        v = {'value': v}
                    if isinstance(v['value'], list):
                        v['value'] = [Fraction(i) for i in v['value']]
                    else:
                        v['value'] = [Fraction(v['value'])]
                    self.children[i] = v


class SameDimensionInputOutput(object):

    def validate(self, gene):
        children = self.validate_children(gene)
        dims = set([str(child.dimension) for child in children])
        if '?' in dims:
            dims.remove('?')
        if len(dims) == 0:
            self.dimension = Dimension.parse('?')
            return
        if '*' in dims:
            dims.remove('*')
            self.dimension = Dimension.parse('*')
            return
        assert len(dims) <= 1
        if len(dims) == 0:
            self.dimension = Dimension.parse('*')
        else:
            self.dimension = Dimension.parse(dims.pop())


class NoDimensionOutput(object):

    def validate(self, gene):
        self.validate_children(gene)
        self.dimension = Dimension()


class SameDimensionOutput(object):

    def __init__(self, child_pos):
        self.child_pos = child_pos

    def validate(self, gene):
        children = self.validate_children(gene)
        self.dimension = children[self.child_pos].dimension.copy()


class UnaryOperator(Operator):

    def __init__(self, name, **kwargs):
        super(UnaryOperator, self).__init__(name, 1, **kwargs)


class SameDimensionInputOutputUnaryOperator(SameDimensionInputOutput, UnaryOperator):

    def __init__(self, name, **kwargs):
        SameDimensionInputOutput.__init__(self)
        UnaryOperator.__init__(self, name, **kwargs)


class NoDimensionOutputUnaryOperator(NoDimensionOutput, UnaryOperator):

    def __init__(self, name, **kwargs):
        NoDimensionOutput.__init__(self)
        UnaryOperator.__init__(self, name, **kwargs)


class BinaryOperator(Operator):

    def __init__(self, name, **kwargs):
        super(BinaryOperator, self).__init__(name, 2, **kwargs)


class SameDimensionInputOutputBinaryOperator(SameDimensionInputOutput, BinaryOperator):

    def __init__(self, name, **kwargs):
        SameDimensionInputOutput.__init__(self)
        BinaryOperator.__init__(self, name, **kwargs)


class NoDimensionOutputBinaryOperator(NoDimensionOutput, BinaryOperator):

    def __init__(self, name, **kwargs):
        NoDimensionOutput.__init__(self)
        BinaryOperator.__init__(self, name, **kwargs)


class SameDimensionOutputLeftBinaryOperator(SameDimensionOutput, BinaryOperator):

    def __init__(self, name, **kwargs):
        SameDimensionOutput.__init__(self, 0)
        BinaryOperator.__init__(self, name, **kwargs)


class SameDimensionOutputRightBinaryOperator(SameDimensionOutput, BinaryOperator):

    def __init__(self, name, **kwargs):
        SameDimensionOutput.__init__(self, 1)
        BinaryOperator.__init__(self, name, **kwargs)


class TernaryOperator(Operator):

    def __init__(self, name, **kwargs):
        super(TernaryOperator, self).__init__(name, 3, **kwargs)


class NoDimensionOutputTernaryOperator(NoDimensionOutput, TernaryOperator):

    def __init__(self, name, **kwargs):
        NoDimensionOutput.__init__(self)
        TernaryOperator.__init__(self, name, **kwargs)
