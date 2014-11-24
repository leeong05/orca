"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
import re
from itertools import product, izip
import inspect
import imp

from orca.alpha.base import BacktestingAlpha
from orca.utils import parallel

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('alpha')
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('--param', type=str)
    parser.add_argument('--file', type=str)
    parser.add_argument('--csvdir', type=str)
    parser.add_argument('--hdf', type=str)
    args = parser.parse_args()

    alpha = args.alpha
    alphaname, ext = os.path.splitext(os.path.basename(alpha))
    assert ext in ('.py', '.pyc')

    module = imp.load_source(alphaname, alpha)
    for name, cls in inspect.getmembers(module):
        if inspect.isclass(cls) and issubclass(cls, BacktestingAlpha):
            alpha = cls
            break

    params = {}
    for k_vs in re.split('[#;]', args.param):
        k, vs = re.split('[=:]', k_vs)
        params[k] = vs.split(',')
    if args.file:
        with open(args.file) as file:
            for line in file:
                k, vs = line.strip().split('\s+')
                if k.lower() == 'startdate':
                    args.startdate = vs
                elif k.lower() == 'enddate':
                    args.enddate = vs
                elif k.lower() == 'param':
                    k, v = vs.split('=')
                    params[k] = v.split(',')

    if params:
        gen = (dict(izip(params, x)) for x in product(*params.itervalues()))
        if args.hdf:
            parallel.run_hdf(args.hdf, alpha, gen, args.start, args.end)
        else:
            parallel.run_csv(args.csvdir, alpha, gen, args.start, args.end)
    else:
        alpha = alpha()
        alpha.run(args.start, args.end)
        alpha.dump(alphaname+'.csv')
