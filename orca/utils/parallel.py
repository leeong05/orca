"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import multiprocessing

def worker(args):
    alpha, param, startdate, enddate = args
    alpha = alpha(param)
    alpha.run(startdate, enddate)
    return alpha.get_alphas()

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


from threading import Lock
import pandas as pd

def worker_hdf(args):
    lock, store, i, alpha, param, startdate, enddate = args
    alpha = alpha(**param)
    alpha.run(startdate, enddate)
    alpha = alpha.get_alphas()
    with lock:
        store['alpha'+str(i)] = alpha
        store.append('params', pd.DataFrame({i: param}).T)
        store.flush(fsync=True)

def run_hdf(store, alpha, params, startdate, enddate, threads=multiprocessing.cpu_count()):
    """Execute instances of an alpha in parallel and stores DataFrame in HDF5 file. Each item in params should be a ``dict``.

    :param store: An HDFStore object from Pandas library
    """
    lock = Lock()
    iterobj = ((lock, store, i, alpha, param, startdate, enddate) for i, param in enumerate(params))
    pool = multiprocessing.Pool(threads)
    pool.imap_unordered(worker_hdf, iterobj)
    pool.close()
    pool.join()
