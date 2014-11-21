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
    :returns: An iterator to get tuple (param, DataFrames). **The returned result may in the same order as ``params``**
    """
    iterobj = ((alpha, param, startdate, enddate) for param in params)
    pool = multiprocessing.Pool(threads)
    res = pool.imap_unordered(worker, iterobj)
    pool.close()
    pool.join()

    return res

import os

import pandas as pd
import warnings
warnings.simplefilter(action = "ignore", category = pd.io.pytables.PerformanceWarning)

from orca.perf.performance import Performance

def worker_hdf(args):
    i, alpha, param, startdate, enddate = args
    alpha = alpha(**param)
    alpha.run(startdate, enddate)
    alpha = alpha.get_alphas()
    return i, param, alpha

def run_hdf(store, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count()):
    """Execute instances of an alpha in parallel and stores DataFrame in HDF5 file. Each item in params should be a ``dict``.

    :param store: File path of the to-be-created HDFStore
    :param function predicate: A function with :py:class:`orca.perf.performance.Performance` object as the only parameter; for example: ``lambda x: x.get_original().get_ir() > 0.1``. Default: None
    """
    if os.path.exists(store):
        os.remove(store)
    logger = get_logger(store)
    store = pd.HDFStore(store)

    iterobj = ((i, alpha, param, startdate, enddate) for i, param in enumerate(params))
    pool = multiprocessing.Pool(threads)
    res = pool.imap_unordered(worker_hdf, iterobj)
    pool.close()
    pool.join()
    for i, param, alpha in res:
        if predicate is not None and not predicate(Performance(alpha)):
            continue
        store['alpha'+str(i)] = alpha
        store.append('params', pd.DataFrame({i: param}).T)
        store.flush()
        logger.debug('Saving alpha with parameter: {!r}'.format(param))
    store.close()
