import unittest

from .dimension import (
        Dimension,
        FacelessDimension,
        BlackholeDimension,
        )


class TestDimension(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_dimension_bool(self):
        d0 = Dimension()
        self.assertFalse(d0)
        d1 = Dimension.parse('c')
        self.assertTrue(d1)

    def test_dimension_str(self):
        d1 = Dimension.parse('d')
        self.assertEqual(str(d1), 'd')
        d2 = Dimension.parse('d**2 // c')
        self.assertEqual(str(d2), 'd**2 // c')

    def test_dimension_repr(self):
        d = Dimension.parse('d**2 // c')
        self.assertEqual(repr(d), repr(d.units))

    def test_dimension_imul(self):
        d1 = Dimension.parse('d**2 // c')
        d2 = Dimension.parse('d**-2 // c**-1')
        d1 *= d2
        self.assertFalse(bool(d1))

    def test_dimension_mul(self):
        d1 = Dimension.parse('d**2 // c')
        d2 = Dimension.parse('d**-2 // c**-1')
        d = d1 * d2
        self.assertFalse(bool(d))
        d3 = FacelessDimension()
        self.assertEqual(d3*d1, d1)
        self.assertEqual(d1*d3, d1)
        d4 = BlackholeDimension()
        self.assertEqual(d4*d1, d4)
        self.assertEqual(d1*d4, d4)
        self.assertEqual(d3*d4, d4)
        self.assertEqual(d4*d3, d4)

    def test_dimension_itruediv(self):
        d1 = Dimension.parse('d**2 // c')
        d2 = d1.copy()
        d1 /= d2
        self.assertFalse(bool(d1))

    def test_dimension_truediv(self):
        d1 = Dimension.parse('d**2 // c')
        d2 = d1.copy()
        d = d1 / d2
        self.assertFalse(bool(d))
        d3 = FacelessDimension()
        self.assertEqual(d3/d1, d1)
        self.assertEqual(d1/d3, d1)
        d4 = BlackholeDimension()
        self.assertEqual(d4/d1, d4)
        self.assertEqual(d1/d4, d4)
        self.assertEqual(d3/d4, d4)
        self.assertEqual(d4/d3, d4)

    def test_dimension_ipow(self):
        d = Dimension.parse('a // b')
        d **= 2
        self.assertEqual(d, Dimension.parse('b**-2 // a**-2'))

    def test_dimension_pow(self):
        d1 = Dimension.parse('a // b')
        d2 = d1 ** 2
        d1 **= 2
        self.assertEqual(d1, d2)
        d3 = FacelessDimension()
        self.assertEqual(d3**2, d3)
        d4 = BlackholeDimension()
        self.assertEqual(d4**2, d4)

    def test_dimension_eq(self):
        d1 = Dimension.parse('d d')
        d2 = Dimension.parse('d**2')
        self.assertEqual(d1, d2)
        self.assertFalse(d1 != d2)
        self.assertEqual(d1, 'd**2')
        self.assertEqual(Dimension(), '')
        self.assertEqual(Dimension.parse('?'), '?')

    def test_faceless_dimension(self):
        d1 = Dimension.parse('?')
        d2 = FacelessDimension()
        self.assertEqual(d1, d2)

    def test_blackhole_dimension(self):
        d1 = Dimension.parse('*')
        d2 = BlackholeDimension()
        self.assertEqual(d1, d2)


if __name__ == '__main__':

    unittest.main()
