"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.logger import get_logger
logger = get_logger('parallel')

import multiprocessing

def worker(args):
    alpha, param, startdate, enddate = args
    alpha = alpha(param)
    alpha.run(startdate, enddate)
    return (param, alpha.get_alphas())

def run(alpha, params, startdate, enddate, threads=multiprocessing.cpu_count()):
    """Execute instances of an alpha in parallel and returns DataFrame in **unordered** manner.

    :param alpha: :py:class:`orca.alpha.base.BacktestingAlpha`
    :param params: Set of parameters to instantiate alpha object, or an iterable object
    :param threads: Number of threads to use in parallel execution
    :returns: An iterator to get DataFrames
    """
    iterobj = ((alpha, param, startdate, enddate) for param in params)
    pool = multiprocessing.Pool(threads)
    res = pool.imap_unordered(worker, iterobj)
    pool.close()
    pool.join()

    return res

import os
from threading import Lock
lock = Lock()

import pandas as pd
import warnings
warnings.simplefilter(action = "ignore", category = pd.io.pytables.PerformanceWarning)

def worker_hdf(args):
    store, i, alpha, param, startdate, enddate = args
    alpha = alpha(**param)
    alpha.run(startdate, enddate)
    alpha = alpha.get_alphas()
    with lock:
        store = pd.HDFStore(store)
        store['alpha'+str(i)] = alpha
        store.append('params', pd.DataFrame({i: param}).T)
        store.flush(fsync=True)

def run_hdf(store, alpha, params, startdate, enddate, threads=multiprocessing.cpu_count()):
    """Execute instances of an alpha in parallel and stores DataFrame in HDF5 file. Each item in params should be a ``dict``.

    :param store: File path of the to-be-created HDFStore
    """
    if os.path.exists(store):
        os.remove(store)
    iterobj = ((store, i, alpha, param, startdate, enddate) for i, param in enumerate(params))
    pool = multiprocessing.Pool(threads)
    pool.imap_unordered(worker_hdf, iterobj)
    pool.close()
    pool.join()

from orca.perf.performance import Performance

def worker_filter_hdf(args):
    store, i, alpha, param, startdate, enddate, predicate = args
    alpha = alpha(**param)
    alpha.run(startdate, enddate)
    alpha = alpha.get_alphas()
    perf = Performance(alpha)
    perf.__name__ = 'perf'
    if eval(predicate.format(perf.__name__)):
        logger.info('Found a passing alpha with parameter: {!r}'.format(param))
        with lock:
            store = pd.HDFStore(store)
            store['alpha'+str(i)] = alpha
            store.append('params', pd.DataFrame({i: param}).T)
            store.flush(fsync=True)

def run_filter_hdf(store, alpha, params, startdate, enddate, predicate, threads=multiprocessing.cpu_count()):
    """Execute instances of an alpha in parallel and stores DataFrame in HDF5 file. Each item in params should be a ``dict``.

    :param store: File path of the to-be-created HDFStore
    # :param predicate: A string that can be parsed into an expression with :py:class:`orca.perf.performance.Performance` object as the only parameter; for example: '{}.get_original().get_ir() > 0.1'
    """
    if os.path.exists(store):
        os.remove(store)
    iterobj = ((store, i, alpha, param, startdate, enddate, predicate) for i, param in enumerate(params))
    pool = multiprocessing.Pool(threads)
    pool.imap_unordered(worker_filter_hdf, iterobj)
    pool.close()
    pool.join()

