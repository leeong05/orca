import bisect
from datetime import datetime

import pandas as pd

def int_to_datetime(dateint):
    return pd.to_datetime(datetime(dateint//10000, (dateint%10000)//100, dateint%100))

def datetime_to_int(datetime):
    return datetime.year*10000 + datetime.month*100 + datetime.day

def to_dateint(dt):
    if type(dt) in (int, pd.Int64Index):
        return dt
    if isinstance(dt, datetime):
        return datetime_to_int(dt)
    if isinstance(dt, pd.DatetimeIndex):
        return pd.Index([datetime_to_int(i) for i in dt])
    return int(dt)

def to_datetime(dt):
    if isinstance(dt, datetime) or type(dt) == pd.DatetimeIndex:
        return dt
    if isinstance(dt, int):
        return int_to_datetime(dt)
    if isinstance(dt, pd.Int64Index):
        return pd.Index([int_to_datetime(i) for i in dt])
    return pd.to_datetime(dt)

def find_ge(dates, date):
    i = bisect.bisect_left(dates, date)
    if i != len(dates):
        return i, dates[i]
    raise IndexError

def find_le(dates, date):
    i = bisect.bisect_right(dates, date)
    if i:
        return i-1, dates[i-1]
    raise IndexError

def shift_date(dates, date, n, direction=None):
    if direction is None:
        direction = -1 if n < 0 else 1
    if direction < 0:
        i = find_le(dates, date)[0]
    else:
        i = find_ge(dates, date)[0]
    if 0 <= i + n <= len(dates)-1:
        return dates[i+n]
    raise IndexError

def adjust_date(dates, date, direction):
    return shift_date(dates, date, 0, direction)

def date_range(dates, date1, date2, backdays=0):
    i1 = find_ge(dates, date1)[0]
    i2 = find_le(dates, date2)[0]
    if 0<= i1-backdays < i2:
        return dates[i1-backdays: i2+1]
    raise IndexError

def get_backward(dates, date, n, offset=0):
    i = find_le(dates, date)[0]
    if i-(n-1)-offset >= 0:
        return dates[i-(n-1)-offset: i+1-offset]
    raise IndexError

def get_forward(dates, date, n, offset=0):
    i = find_ge(dates, date)[0]
    if i+n+offset <= len(dates):
       return dates[i+offset: i+n+offset]
    raise IndexError
