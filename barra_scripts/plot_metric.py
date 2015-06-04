"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""
import pandas as pd

import logbook
logbook.set_datetime_format('local')
logger = logbook.Logger('plotter')

from orca.utils.io import read_frame
from orca.utils.plot import (plot_ts,
    save_figs,
    )


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('metric', type=str, nargs='+')
    parser.add_argument('--which', type=str, nargs='*')
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--legend', type=str, nargs='*')
    parser.add_argument('--pdf', type=str, required=True)
    args = parser.parse_args()

    if not args.legend or len(args.legend) != len(args.position):
        args.legend = range(len(args.position))

    metrics = []
    for fname in args.metric:
        metric = read_frame(fname)
        if args.all:
            args.which = metric.columns
        metrics.append(metric)

    figs = []
    for col in args.which:
        if col not in metrics[0].columns:
            continue
        df = []
        for metric in metrics:
            df.append(metric[col])
        if len(df) > 1:
            df = pd.concat(df, axis=1)
            df.columns = args.legend
            df.columns.name = col
        else:
            df = df[0]
        fig = plot_ts(df)
        figs.append(fig)
    save_figs(figs, args.pdf)
