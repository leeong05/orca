"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import subprocess

from orca import DATES
from orca.utils import dateutil

def generate_dates(startdate, enddate, num):
    if enddate is None:
        enddate = DATES[-1]
    dates = dateutil.cut_window(
            DATES,
            dateutil.compliment_datestring(str(startdate), -1, True),
            dateutil.compliment_datestring(str(enddate), 1, True)
            )
    chksize = len(dates) / num + (len(dates) % num > 0)
    return [dates[i: i+chksize] for i in range(0, len(dates), chksize)]

def run(script, start, end, num, offset=None, skip_monitor=False):
    progs = []
    for dates in generate_dates(start, end, num):
        cmd = ['python', script, '--logoff', '-s', dates[0], '-e', dates[-1]]
        if offset is not None:
            cmd +=  ['--offset', str(offset)]
        if skip_monitor:
            cmd += ['--skip_monitor']
        prog = subprocess.Popen(cmd)
        progs.append(prog)

    for prog in progs:
        prog.wait()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('script', type=str)
    parser.add_argument('-s', '--start', type=str, required=True)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('-n', '--num', type=int, default=8)
    parser.add_argument('--offset', type=int, default=None)
    parser.add_argument('--skip_monitor', action='store_true')
    args = parser.parse_args()

    run(args.script, args.start, args.end, args.num, args.offset, args.skip_monitor)
