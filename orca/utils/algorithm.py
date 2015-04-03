"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from copy import copy

def merge_intervals(intervals):
    intervals = copy(intervals)
    if len(intervals) <= 1:
        return intervals
    intervals.sort(key=lambda x: x[0])
    stack = [intervals[0]]
    for i in range(1, len(intervals)):
        top = stack[-1]
        if top[1] < intervals[i][0]:
            stack.append(intervals[i])
        elif top[1] < intervals[i][1]:
            stack.pop()
            stack.append((top[0], intervals[i][1]))
    return stack

def cut_interval(interval, exit_series):
    ser = exit_series.ix[interval[0]:interval[1]]
    intervals = []
    while len(ser) > 0:
        start, end = None, None
        for dt, value in ser.iteritems():
            if not value:
                if start is None:
                    start = dt
                    end = dt
                else:
                    end = dt
                    if ser.index.get_loc(end)+1 == len(ser):
                        intervals.append((start, end))
            else:
                if start is not None:
                    intervals.append((start, end))
                    break
        if end is None:
            ser = []
        else:
            ser = ser.iloc[ser.index.get_loc(end)+1:]
    return intervals

def get_intervals(entry_series, n, exit_series=None):
    intervals = []
    for i, index in enumerate(entry_series.index):
        if entry_series.ix[index]:
            intervals.append((index, i+n < len(entry_series) and entry_series.index[i+n] or entry_series.index[-1]))
    intervals = merge_intervals(intervals)
    stack = [intervals[0]]
    for i in range(1, len(intervals)):
        top = stack[-1]
        if entry_series.index.get_loc(top[1])+1 == entry_series.index.get_loc(intervals[i][0]):
            stack.pop()
            stack.append((top[0], intervals[i][1]))
        else:
            stack.append(intervals[i])
    if exit_series is None:
        return stack
    res = []
    for interval in stack:
        for tres in cut_interval(interval, exit_series):
            res.append(tres)
    return res
