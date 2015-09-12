import numpy as np
import pandas as pd

def almost_zero(obj, **kwargs):
    try:
        np.testing.assert_array_almost_equal(obj, 0, **kwargs)
        return True
    except:
        return False

def almost_equal(obj, value, **kwargs):
    return almost_zero(obj-value, **kwargs)

def series_equal(df1, df2, **kwargs):
    try:
        pd.util.testing.assert_series_equal(df1, df2, **kwargs)
        return True
    except:
        return False

def frame_equal(df1, df2, **kwargs):
    try:
        pd.util.testing.assert_frame_equal(df1, df2, **kwargs)
        return True
    except:
        return False

