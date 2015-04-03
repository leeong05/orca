"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

def twinx_plot(y1, y2, params1={'color': 'red'}, params2={'color': 'blue'}):
    dates = y1.index.union(y2.index)
    y1, y2 = y1.reindex(index=dates), y2.reindex(index=dates)
    fig, ax = plt.subplots()
    ax.plot(dates, y1, **params1)
    ax.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    ax.set_ylabel(y1.name or 'y1', color=params1.get('color', 'red'))
    fig.autofmt_xdate()

    ax = ax.twinx()
    ax.plot(dates, y2, **params2)
    ax.set_ylabel(y2.name or 'y2', color=params2.get('color', 'blue'))
    return fig

def plot_intervals(ser, intervals):
    fig, ax = plt.subplots()
    ax.plot(ser.index, ser)
    ax.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    for interval in intervals:
        ser_slice = ser.ix[interval[0]: interval[1]]
        ax.plot(ser_slice.index, ser_slice, 'r')
    fig.autofmt_xdate()
    return fig
