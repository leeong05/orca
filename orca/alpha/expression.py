from orca.alpha.base import BacktestingAlpha

class ExpressionAlpha(BacktestingAlpha):
    """Base class for alphas constructed from expressions.

    :param str expression: alpha expression

    .. note::

       This is a base class and should not be used directly.
    """

    def __init__(self, expression, *args, **kwargs):
        self.expression = self.parse_expression(expression)
        BacktestingAlpha.__init__(self, *args, **kwargs)

    @staticmethod
    def parse_expression(self, expression):
        """Parse the expression string into Python expressions."""
        raise NotImplementedError
