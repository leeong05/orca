"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import logbook
logbook.set_datetime_format('local')

logger = logbook.Logger('parallel')

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
import shutil
import json
import cPickle
import msgpack

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
    logger = logbook.Logger(store)
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

def worker_separate_file(args):
    i, alpha, param, startdate, enddate = args
    alpha = alpha(**param)
    alpha.run(startdate, enddate)
    alpha = alpha.get_alphas()
    return i, param, alpha

def run_separate_file(outdir, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count(), ftype='csv'):
    """Execute instances of an alpha in parallel and stores each DataFrame in separate file. Each item in params should be a ``dict``.

    :param outdir: Diretory to store output files
    :param function predicate: A function with :py:class:`orca.perf.performance.Performance` object as the only parameter; for example: ``lambda x: x.get_original().get_ir() > 0.1``. Default: None
    :param str ftype: File format; currently only supports ('csv', 'pickle', 'msgpack')
    """
    if os.path.exists(outdir) and os.path.isdir(outdir):
        shutil.rmtree(outdir)
    logger = logbook.Logger(outdir)
    os.makedirs(outdir)

    iterobj = ((i, alpha, param, startdate, enddate) for i, param in enumerate(params))
    pool = multiprocessing.Pool(threads)
    res = pool.imap_unordered(worker_hdf, iterobj)
    pool.close()
    pool.join()
    params = {}
    for i, param, alpha in res:
        if predicate is not None and not predicate(Performance(alpha)):
            continue
        params[i] = param
        logger.debug('Saving alpha with parameter: {!r}'.format(param))
        if ftype == 'csv':
            alpha.to_csv(os.path.join(outdir, 'alpha'+str(i)))
        elif ftype == 'pickle':
            alpha.to_pickle(os.path.join(outdir, 'alpha'+str(i)))
        elif ftype == 'msgpack':
            alpha.to_msgpack(os.path.join(outdir, 'alpha'+str(i)))

    with open(os.path.join(outdir, 'params.json'), 'w') as file:
        if ftype == 'csv':
            json.dump(params, file)
        elif ftype == 'pickle':
            cPickle.dump(params, file)
        elif ftype == 'msgpack':
            msgpack.dump(params, file)

def run_csv(outdir, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count()):
    return run_separate_file(outdir, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count(), ftype='csv')

def run_pickle(outdir, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count()):
    return run_separate_file(outdir, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count(), ftype='pickle')

def run_msgpack(outdir, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count()):
    return run_separate_file(outdir, alpha, params, startdate, enddate, predicate=None, threads=multiprocessing.cpu_count(), ftype='msgpack')


def worker_daily(args):
    alpha, date = args
    res = alpha.generate(date)
    alpha.debug('Generated alpha for {}'.format(date))
    return date, res

def run_daily(alpha, startdate, enddate, dates=None, threads=multiprocessing.cpu_count()):
    if dates is None:
        dates = alpha.generate_dates(startdate, enddate)

    pool = multiprocessing.Pool(threads)
    res = pool.imap_unordered(worker_daily, ((alpha, date) for date in dates))
    pool.close()
    pool.join()
    for k, v in res:
        alpha[k] = v

def worker_interval(args):
    alpha, date = args
    res = {}
    for time in alpha.times:
        dt_alpha = alpha.generate(date, time)
        res[(date, time)] = dt_alpha
    alpha.debug('Generated alpha for {}'.format(date))
    return res

def run_interval(alpha, startdate, enddate, dates=None, threads=multiprocessing.cpu_count()):
    if dates is None:
        dates = alpha.generate_dates(startdate, enddate)

    pool = multiprocessing.Pool(threads)
    res = pool.imap_unordered(worker_interval, ((alpha, date) for date in dates))
    pool.close()
    pool.join()
    for kvs in res:
        for k, v in kvs.iteritems():
            alpha[k] = v

def worker_chunk(args):
    alpha = args[0](*args[1])
    alpha.run(dates=args[1][0])
    return alpha.get_alphas()

def run_chunk(alpha, startdate, enddate, chksize, args=(), threads=multiprocessing.cpu_count()):
    pool = multiprocessing.Pool(threads)
    res = pool.imap_unordered(worker_chunk, ((alpha, (dates,) + args) for dates in alpha.generate_dates(startdate, enddate, chksize)))
    pool.close()
    pool.join()
    df = []
    for chk in res:
        df.append(chk)
    df = pd.concat(df).sort_index()
    return df
