"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from pandas.util.testing import (
        assert_series_equal,
        assert_frame_equal,
        assert_panel_equal)

def series_equal(s1, s2):
    try:
        assert_series_equal(s1, s2)
        return True
    except AssertionError:
        return False

def frames_equal(df1, df2):
    try:
        assert_frame_equal(df1, df2)
        return True
    except AssertionError:
        return False

def panels_equal(pl1, pl2):
    try:
        assert_panel_equal(pl1, pl2)
        return True
    except AssertionError:
        return False
