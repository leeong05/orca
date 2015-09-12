from fractions import Fraction
from collections import defaultdict
from copy import copy


class Dimension(object):

    def __init__(self, units=None):
        if units:
            self.units = units
        else:
            self.units = defaultdict(Fraction)

    @staticmethod
    def parse(string):
        if not string:
            return Dimension()

        if isinstance(string, Dimension):
            return string.copy()

        if string.strip() == '?':
            return FacelessDimension()

        if string.strip() == '*':
            return BlackholeDimension()

        units = defaultdict(Fraction)
        tokens = string.split('//')
        assert len(tokens) <= 2
        for i in tokens[0].strip().split():
            i = i.strip()
            try:
                u, f = i.split('**')
                f = Fraction(f)
                if f != 0:
                    units[u] += f
            except:
                units[i] += Fraction(1)
        if len(tokens) == 2:
            for i in tokens[1].strip().split():
                i = i.strip()
                try:
                    u, f = i.strip().split('**')
                    f = Fraction(f)
                    if f != 0:
                        units[u] -= f
                except:
                    units[i] -= Fraction(1)
        return Dimension(units)

    def __str__(self):
        if isinstance(self.units, str):
            return self.units

        if not self.units:
            return ''

        numerator, denominator = [], []
        for k, v in self.units.items():
            if v > 0:
                if v == 1:
                    numerator.append('%s' % k)
                else:
                    numerator.append('%s**%s' % (k, v))
            else:
                if v == -1:
                    denominator.append('%s' % k)
                else:
                    denominator.append('%s**%s' % (k, -v))
        numerator.sort()
        denominator.sort()
        numerator = ' '.join(numerator)
        denominator = ' '.join(denominator)

        if denominator:
            return ' // '.join([numerator, denominator])
        else:
            return numerator

    def __repr__(self):
        return repr(self.units)

    def __imul__(self, other):
        if isinstance(other, FacelessDimension):
            return self
        if isinstance(other, BlackholeDimension):
            return BlackholeDimension()
        if isinstance(self, FacelessDimension):
            return other.copy()
        if isinstance(self, BlackholeDimension):
            return self

        for k, v in other.units.items():
            self.units[k] += v

        units = defaultdict(Fraction)
        for k, v in self.units.items():
            if v != 0:
                units[k] = v
        self.units = units
        return self

    def __mul__(self, other):
        result = self.copy()
        result *= other
        return result

    def __itruediv__(self, other):
        if isinstance(other, FacelessDimension):
            return self
        if isinstance(other, BlackholeDimension):
            return BlackholeDimension()
        if isinstance(self, FacelessDimension):
            self.units = copy(other.units)
            return self
        if isinstance(self, BlackholeDimension):
            return self

        for k, v in other.units.items():
            self.units[k] -= v

        units = defaultdict(Fraction)
        for k, v in self.units.items():
            if v != 0:
                units[k] = v
        self.units = units
        return self

    def __truediv__(self, other):
        result = self.copy()
        result /= other
        return result

    def __ipow__(self, exp):
        if isinstance(self, FacelessDimension):
            return self
        if isinstance(self, BlackholeDimension):
            return self

        for k, _ in self.units.items():
            self.units[k] *= exp
        return self

    def __pow__(self, exp):
        result = self.copy()
        result **= exp
        return result

    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return not (self == other)

    def __bool__(self):
        return bool(self.units)

    def copy(self):
        return Dimension.parse(str(self))


class FacelessDimension(Dimension):

    def __init__(self):
        super(Dimension, self).__init__()
        self.units = '?'


class BlackholeDimension(Dimension):

    def __init__(self):
        super(Dimension, self).__init__()
        self.units = '*'
