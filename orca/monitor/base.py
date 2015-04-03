"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import abc

import mysql.connector
import logbook
logbook.set_datetime_format('local')


class MonitorFetcherBase(object):

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = 'monitor'
    MONITOR = mysql.connector.connect(
            host='192.168.1.183',
            user='kbars',
            password='123456',
            database='monitor',
            )

    @classmethod
    def connect_monitor(cls):
        cls.MONITOR = mysql.connector.connect(
                host='192.168.1.183',
                user='kbars',
                password='123456',
                database='monitor',
                )

    def __init__(self, datetime_index=False, date_check=False, delay=1, **kwargs):
        self.logger = logbook.Logger(MonitorFetcherBase.LOGGER_NAME)
        self.datetime_index = datetime_index
        self.date_check = date_check
        self.delay = delay
        self.__dict__.update(kwargs)

    @abc.abstractmethod
    def fetch(self, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_window(self, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_history(self, *args, **kwargs):
        raise NotImplementedError
