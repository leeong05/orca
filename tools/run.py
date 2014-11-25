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
    parser.add_argument('alpha', help='Alpha .py file')
    parser.add_argument('-s', '--start', type=str, help='Simulation starting date')
    parser.add_argument('-e', '--end', type=str, help='Simulation ending date')
    parser.add_argument('--param', type=str, help='A quoted string to supply keyword parameters; for example: --param=\'p1=p1v1,p1v2;p2=p2v1,p2v2.p3v3\'')
    parser.add_argument('--file', type=str, help='A file containing configuration parameters(A .py file is recommended)')
    parser.add_argument('--csvdir', type=str, help='Diretory to dump generated DataFrames')
    parser.add_argument('--hdf', type=str, help='HDF5 file name to save generated DataFrames')
    args = parser.parse_args()

    alpha = args.alpha
    alphaname, ext = os.path.splitext(os.path.basename(alpha))
    assert ext in ('.py', '.pyc')

    module = imp.load_source(alphaname, alpha) if ext == '.py' else imp.load_compiled(alphaname, alpha)
    for name, cls in inspect.getmembers(module):
        if inspect.isclass(cls) and cls is not BacktestingAlpha and issubclass(cls, BacktestingAlpha):
            alpha = cls
            break

    params = {}
    if args.param:
        for k_vs in re.split('[#;]', args.param):
            k, vs = re.split('[=:]', k_vs)
            params[k] = vs.split(',')
    if args.file:
        filename, ext = os.path.splittext(os.path.basename(args.file))[1]
        if ext in ('.py', '.pyc'):
            param = imp.load_source(filename, args.file) if ext == '.py' else imp.load_compiled(filename, args.file)
            content = dir(param)
            if 'startdate' in content:
                args.startdate = param.startdate
            if 'enddate' in content:
                args.enddate = param.enddate
            if 'params' in content:
                params.update(param.params)
        else:
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
            assert args.csvdir is not None
            parallel.run_csv(args.csvdir, alpha, gen, args.start, args.end)
    else:
        alpha = alpha()
        alpha.run(args.start, args.end)
        alpha.dump(alphaname+'.csv')
