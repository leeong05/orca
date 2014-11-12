import os
from StringIO import StringIO
from datetime import datetime
import argparse
import logging
from multiprocessing import Process

import pandas as pd
import lockfile
from pymongo import MongoClient
import cx_Oracle

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s@%(asctime)s]: %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger('updater')

"""
The base class for updaters
"""

class UpdaterBase(object):

    def __init__(self, timeout=600):    # 10 minutes
        self.timeout = timeout
        self.connected = False

    def connect_mongo(self, host='192.168.1.183', db='stocks_dev',
            user='stocks_dev', password='stocks_dev'):
        client = MongoClient(host)
        db = client[db]
        db.authenticate(user, password)
        self.__dict__.update({'client': client, 'db': db})
        self.connected = True
        logger.debug('Connected to %s using database %s', host, db)

    def disconnect_mongo(self):
        self.client.close()

    def update(self, date):
        raise NotImplementedError

    def pre_update(self):
        pass

    def pro_update(self):
        pass

    def connect_jydb(self):
        connection = cx_Oracle.connect('jydb/jydb@jydb')
        cursor = connection.cursor()
        logger.debug('Connected to Oracle Database jydb/jydb@jydb')
        self.__dict__.update({'connection': connection, 'cursor': cursor})

    def connect_zyyx(self):
        connection = cx_Oracle.connect('zyyx/zyyx@zyyx')
        cursor = connection.cursor()
        logger.debug('Connected to Oracle Database zyyx/zyyx@zyyx')
        self.__dict__.update({'connection': connection, 'cursor': cursor})

    def run(self):
        self.parse_args()
        if not self.connected:
            self.connect_mongo()
        self.pre_update()
        for date in self._dates:
            if hasattr(self, 'dates') and date not in self.dates:
                continue
            logger.info('START')
            p = Process(target=self.update, args=(date,))
            p.start()
            p.join(self.timeout)
            if p.is_alive():
                logger.warning('Too much time spent on date: %s', date)
                p.terminate()
            logger.info('END\n')
        self.pro_update()
        self.disconnect_mongo()
        self.save_logfile()

    def parse_args(self):
        today = datetime.now().strftime('%Y%m%d')

        parser = argparse.ArgumentParser()
        parser.add_argument('-o', '--logoff', help='turnoff logfile', action='store_true')
        parser.add_argument('-s', '--start', help='start date(included)', type=str)
        parser.add_argument('-e', '--end', help='end date(included); default: today', default=today, nargs='?')
        parser.add_argument('date', help='the date to be updated', default=today, nargs='?')
        parser.add_argument('-f', '--logfile', help='the log file name', type=str)
        args = parser.parse_args()

        if args.logfile:
            args.logoff = False

        if not args.logoff and not args.logfile:
            logger.debug('@logfile not explicitly provided')
            logdir = 'logs/%s/%s' % (today[:4], today[4:6])
            if not os.path.exists(logdir):
                os.makedirs(logdir)
                logger.debug('Created directory %s', logdir)
            args.logfile = os.path.join(logdir, 'log.'+today)
            logger.debug('@logfile set to: %s', args.logfile)
        if args.start and args.end:
            _dates = [dt.strftime('%Y%m%d') for dt in pd.date_range(args.start, args.end)]
        else:
            _dates = [args.date]

        self.__dict__.update({
            'logoff': args.logoff,
            'logfile': args.logfile,
            'today': today,
            '_dates': _dates,
            })

        if not args.logoff:
            log_str = StringIO()
            str_hdl = logging.StreamHandler(log_str)
            str_hdl.setLevel(logging.INFO)
            str_fmt = logging.Formatter(fmt='%(filename)s: %(levelname)s@%(asctime)s]: %(message)s')
            str_hdl.setFormatter(str_fmt)
            logger.addHandler(str_hdl)
            logger.debug('Added a StringIO channel to logging')
            self.__dict__.update({'log_str': log_str})

    def save_logfile(self):
        if not self.logoff:
            with lockfile.LockFile(self.logfile):
                with open(self.logfile, 'a') as file:
                    file.write(self.log_str.getvalue())
