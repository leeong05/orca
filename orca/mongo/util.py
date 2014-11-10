"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import bisect
from datetime import datetime, timedelta, time
from calendar import monthrange

def is_sorted(l, ascending=True):
    """Check if a list is sorted."""
    if ascending:
        return all(l[i] <= l[i+1] for i in xrange(len(l)-1))
    return all(l[i] >= l[i+1] for i in xrange(len(l)-1))

def find_ge(l, x):
    """Find the leftmost item in the sorted(**ascending**) list >= ``x``."""
    i = bisect.bisect_left(l, x)
    if i != len(l):
        return i, l[i]
    raise ValueError('No item in the list >= {0!r}'.format(x))

def find_le(l, x):
    """Find the rightmost item in the sorted(**ascending**) list <= ``x``"""
    i = bisect.bisect_right(l, x)
    if i:
        return i-1, l[i-1]
    raise ValueError('No item in the list <= {0!r}'.format(x))

def parse_date(dates, date, direction=1):
    """Find the item in ``dates`` that is closest(specified by ``direction``) to ``date``

    :param list dates: The pre-defined list, must be sorted in ascending order
    :param date: ``date`` may be already in ``dates``; otherwise, it will be parsed into another item in the list
    :param int direction: 1(default): the desired item >= ``date``;-1: the desired item <= ``date``

    :returns: (i, dates[i]), ``i`` is the index of the desired item in ``dates``

    """
    if date in dates:
        return dates.index(date), date

    if direction == -1:
        return find_le(dates, date)
    else:
        return find_ge(dates, date)

def compliment_datestring(datestr, direction=-1, date_check=False):
    """Compliment a 4- or 6-length date string into 8-length in format ``yyyymmdd``.

    :param str datestr: For example, '2014', '201401' or '20140101'
    :param int direction: Default: 1
        * -1(default): compliment as the minimal possible
        * 1: compliment as the maximal possible
    :param boolean date_check: Whether to check if ``datestr`` is a valid date string. Default: False

    """
    if len(datestr) == 8:
        if not date_check:
            return datestr

        try:
            return datetime.strptime(datestr, '%Y%m%d').strftime('%Y%m%d')
        except:
            raise ValueError('%s is not a valid argument', datestr.__repr__())
    elif len(datestr) == 6:
        if not date_check:
            return datestr + '31' if direction == 1 else datestr + '01'

        try:
            year, month = int(datestr[:4]), int(datestr[4:6])
            day = monthrange(year, month)[1]
            return '{0:4d}{1:02d}{2:02d}'.format(year, month, day)
        except:
            raise ValueError('{0!r} is not a valid argument'.format(datestr))
    elif len(datestr) == 4:
        return datestr + '1231' if direction == 1 else datestr + '0101'
    else:
        raise ValueError('{0!r} is not a valid argument'.format(datestr))

def cut_window(dates, startdate, enddate=None, backdays=0):
    """Cut out the portion of a list specified by left and right endpoints.

    :param list dates: The pre-defined list, must be sorted in ascending order
    :param startdate: The *left* (may not be the actual) cut-out point
    :param enddate: The right cut-out point. Default: None, defaults to the last item in the list
    :param int backdays: This will shift (left/right: >/< 0) the left cut-out point. Default: 0
    :returns: The cut-out portion
    :rtype: list

    """
    startindex, startdate = parse_date(dates, startdate, 1)
    if startindex < backdays:
        raise ValueError('Cannot left-shift {0!r} with step {1} in the list'.format(startdate, backdays))

    if enddate is None:
        enddate = dates[-1]
    endindex, enddate = parse_date(dates, enddate, -1)
    return dates[startindex-backdays: endindex+1]

_dummy_date = datetime(1900, 1, 1)
def generate_timestamps(starttime, endtime, step, end_excluded=True):
    """Generate a consecutive time stamps.

    :param str starttime, endtime: Must be 6-length time string in the format 'hhmmss'
    :param int step: Number of **seconds** as step
    :param boolean end_excluded: Whether the ``endtime`` itself should be excluded from the result. Default: True
    :returns: Python generator

    """

    starttime = time(int(starttime[:2]), int(starttime[2:4]), int(starttime[4:6]))
    step = timedelta(seconds=int(step))
    dt = datetime.combine(_dummy_date, starttime)

    while True:
        timestamp = dt.strftime('%H%M%S')
        if (end_excluded and timestamp < endtime) or \
                (not end_excluded and timestamp <= endtime):
            yield timestamp
            dt += step
        else:
            break
