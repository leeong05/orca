from hashlib import md5

from lxml import etree

from .dimension import Dimension


class Gene(object):

    def __init__(self, sequence=[]):
        self.sequence = sequence
        self.__pos, self._pos = 0, 0

    def __len__(self):
        return len(self.sequence)

    @property
    def length(self):
        return len(self)

    def __bool__(self):
        return bool(len(self))

    def __depth(self):
        node = self.next_node()
        children = []
        for _ in range(len(node.children)):
            children.append(self.__depth())
        if children:
            return 1 + max(children)
        else:
            return 1

    @property
    def depth(self):
        try:
            return self.__d
        except AttributeError:
            self.__d = self.__depth()
            self.reset()
            return self.__d

    def __string(self):
        node = self.next_node()
        children = []
        for _ in range(len(node.children)):
            children.append(self.__string())
        if children:
            return ''.join([str(node), '(', ', '.join(children), ')'])
        else:
            return str(node)

    def __str__(self):
        try:
            return self.__s
        except AttributeError:
            self.__s = self.__string()
            self.reset()
            return self.__s

    @property
    def string(self):
        return str(self)

    def __xml(self):
        node = self.next_node()
        for _ in range(len(node.children)):
            node.xml.append(self.__xml())
        return node.xml

    @property
    def xml(self):
        try:
            return self.__x
        except AttributeError:
            self.__x = self.__xml()
            self.reset()
            return self.__x

    def to_xmlstring(self, pretty=False):
        return etree.tostring(self.xml, pretty_print=pretty)

    @property
    def dimension(self):
        try:
            return self.__dim
        except AttributeError:
            if self.validate():
                self.__dim = self.sequence[0].dimension
            else:
                self.__dim = Dimension()
            return self.__dim

    @property
    def hashcode(self):
        return md5(str(self).encode('utf-8')).hexdigest()

    def __eq__(self, other):
        return self.hashcode == other.hashcode

    def __neq__(self, other):
        return self.hashcode != other.hashcode

    def reset(self):
        self.__pos = 0

    def next_node(self):
        res = self.sequence[self.__pos]
        self.__pos += 1
        return res

    def __next_sibling_pos(self):
        node = self.sequence[self._pos]
        self._pos += 1
        for _ in range(len(node.children)):
            self.__next_sibling_pos()

    def _next_sibling_pos(self, pos):
        self._pos = pos
        self.__next_sibling_pos()
        pos, self._pos = self._pos, 0
        return pos

    def next_sibling(self, pos=0):
        pos1, pos2 = pos, self._next_sibling_pos(pos)
        return Gene(self.sequence[pos1: pos2])

    def copy(self):
        return self.next_sibling()

    def next_value(self, environment, date1, date2):
        node = self.next_node()
        return node.eval(environment, self, date1, date2)

    def eval(self, environment, date1, date2):
        try:
            res = self.next_value(environment, date1, date2)
            return res
        finally:
            self.reset()

    def validate(self):
        try:
            return self.__v
        except AttributeError:
            pass
        try:
            node = self.next_node()
            node.validate(self)
            self.__v = True
        except AssertionError:
            self.__v = False
        finally:
            self.reset()
            return self.__v

    def replace(self, pos, other):
        pos1, pos2 = pos, self._next_sibling_pos(pos)
        res = Gene()
        if isinstance(other, Gene):
            other = other.sequence
        res.sequence = self.sequence[0: pos1] + other + self.sequence[pos2: ]
        return res
