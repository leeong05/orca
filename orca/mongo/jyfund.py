"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from threading import Lock
import multiprocessing

import numpy as np
import pandas as pd

from orca import (
        DB,
        DATES,
        SIDS,
        )
from orca.utils import dateutil

from base import KDayFetcher

YEAR, SEMI, QUARTER = 4, 2, 1

def myfunc1(args):
    sid, df, quarter_offset = args
    df = df.drop_duplicates('qtrno', take_last=True)
    df.index = df.qtrno
    qtr = df.qtrno.iloc[-1] - quarter_offset
    try:
        return {sid: df.ix[qtr]}
    except:
        return {sid: None}

def myfunc2(args):
    sid, df, table, rtype, use_adjust = args
    df.index = range(0, len(df))

    def myapply(df):
        """when we have different reports for the same quarter, if we want to use adjusted data,
        we pretend that the last report date is the first report date"""
        if len(df) == 1:
            return df.iloc[0]
        date = df.date.iloc[0]
        adjust = df.fillna(method='ffill').iloc[-1].copy()
        adjust.date = date
        return adjust

    if use_adjust == True:
        df = df.groupby('qtrno').apply(myapply)
        df.sort(['date', 'qtrno'], inplace=True)
        df.index = range(0, len(df))
        if not dateutil.is_sorted(df.qtrno):
            return {sid: None}
    elif use_adjust == False:
        df = df.drop_duplicates('qtrno', take_last=False)
        df.index = range(0, len(df))
        if not dateutil.is_sorted(df.qtrno):
            return {sid: None}

    if table == 'balancesheet':
        df = df.sort(['date', 'qtrno'])
        qtrno, res = None, {}
        for _, row in df.iterrows():
            if qtrno is None or row['qtrno'] >= qtrno:
                res[row['date']] = row
                qtrno = row['qtrno']
        return {sid: pd.DataFrame(res)}

    res = {}
    for i in range(len(df)-1, 0, -1):
        """when row['date'] is already in res, it means multiple reports for different quarters
        we only record the report for the most recent quarter
        """
        row = df.iloc[i].copy()
        if row['date'] in res:
            continue

        """for quarterly data, only 0331 does not need to be subtracted;
        for semi-annual data, only 0630 does not need to be subtracted
        """
        if rtype != YEAR and row['qtrno'] % 4 != rtype:
            prev_qtrno = row['qtrno'] - rtype
            """missing points?"""
            if prev_qtrno not in df.qtrno.values:
                continue
            """when use_adjust is True/False, the qtrno column is sorted and unique;
            if use_adjust is None, the situation is more complicated"""
            if use_adjust in (True, False):
                j = list(df.qtrno).index(prev_qtrno)
            else:
                for k in range(i-1,-1,-1):
                    if prev_qtrno == df.qtrno.iloc[k]:
                        j = k
                        break
            prev_row = df.iloc[j]
            for k, v in row.iterkv():
                if k not in ('date', 'sid', 'qtrno'):
                    row[k] = v - prev_row[k]
        res[row['date']] = row
    return {sid: pd.DataFrame(res)}

def myfunc3(args):
    sid, df, records, rtype, dname, table = args
    df = df.drop_duplicates('qtrno', take_last=True)
    df.index = df.qtrno
    max_qtr = df.qtrno.iloc[-1]
    qtrs = [max_qtr - i*rtype for i in range(records)]
    try:
        res = df[dname].ix[qtrs]
    except:
        res = None
    if res is None:
        return {sid: res}
    if table == 'balancesheet':
        res.index = range(records)
        return {sid: res}

    if rtype != YEAR:
        for qtr in qtrs:
            if qtr % 4 != rtype:
                pqtr = qtr - rtype
                try:
                    res.ix[qtr] -= df[dname].ix[pqtr]
                except:
                    res.ix[qtr] = None
    res.index = range(records)
    return {sid: res}


class JYFundFetcher(KDayFetcher):
    """Class to fetch JYDB fundamental data.

    :param str table: Table name, must be one of ('balancesheet', 'income', 'cashflow', 'data')
    :param startyear: Fetch data starting from this year in advance, must be in the format 'YYYY'. Default: 2007
    :type startyear: str, int
    """

    tables = ('balancesheet', 'income', 'cashflow', 'data')
    collections = {
            'balancesheet': DB.jybs,
            'income': DB.jyis,
            'cashflow': DB.jycs,
            'data': DB.jydt,
            }

    datas, startyears = {}, {}

    mongo_lock = Lock()

    @classmethod
    def get_data(cls, table, startyear=None):
        if table in cls.datas and (startyear is None or int(startyear) >= cls.startyears[table]):
            return cls.datas[table]
        return cls.get_data_mongo(table, startyear=startyear)

    @classmethod
    def get_data_mongo(cls, table, startyear=None):
        with cls.mongo_lock:
            if startyear is None:
                startyear = 2007
            query = {'year': {'$gte': startyear}}
            proj = {'_id': 0}
            df = pd.DataFrame(list(cls.collections[table].find(query, proj)))
            df['qtrno'] = (df.year - 2000) * 4 + df.quarter
            cls.datas[table], cls.startyears[table] = df, startyear
        return cls.datas[table]

    @classmethod
    def set_data(cls, table, df):
        """Use this method to set data so that for future uses, it is not necessary to interact with MongoDB."""
        with cls.mongo_clock:
            cls.datas[table] = df
            cls.startyears[table] = df.year.min()

    def __init__(self, table, startyear=2007, **kwargs):
        if table not in JYFundFetcher.tables:
            raise ValueError('No such table {0!r} exists of JYDB fundamental data'.format(table))
        self._table = table
        self._startyear = int(startyear)
        super(JYFundFetcher, self).__init__(**kwargs)

    @property
    def table(self):
        """Property."""
        return self._table

    @table.setter
    def table(self, args):
        if isinstance(args, tuple):
            table, startyear = args
        else:
            table, startyear = args, None
        if table not in JYFundFetcher.tables:
            self.warning('No such table {0!r} exists of JYDB fundamental data. Nothing has changed'.format(table))
            return
        self._table = table
        if startyear is not None:
            self._startyear = int(startyear)

    def load(self):
        """Call this method to instruct data preparation."""
        JYFundFetcher.get_data(self.table, self._startyear)

    def prepare_frame(self, dnames=None, date=None, quarter_offset=0, rtype=QUARTER, quarter=None, **kwargs):
        """Prepare cross-sectional data items and concatenate them into a DataFrame, using data with timestamps less than ``date`` minus ``delay``.

        :param list dnames: List of data names. Default: None, all data items will be included(**NOT** recommended)
        :param int quarter_offset: Offset in quarter numbers. Default: 0
        :param enum rtype: What type of reports to be considered? QUARTER(1, default): all reports; SEMI(2): only semi-annual reports and annual reports; YEAR(4): only annual reports
        :param int quarter: It will only fetch data for this particular quarter(the 4 quarters in a year is 1, 2, 3, 4), thus should be compatible with the use of ``rtype``. Default: None, fetch whatever data is available

        .. seealso:: :py:meth:`JYFundFetcher.prepare_panel`
        """
        if quarter is not None:
            try:
                assert quarter % rtype == 0
            except AssertionError:
                raise ValueError('rtype {0!r} and quarter {1!r} are not compatible')

        date_check = kwargs.get('date_check', self.date_check)
        reindex = kwargs.get('reindex', self.reindex)
        delay = kwargs.get('delay', self.delay)

        di, date = dateutil.parse_date(DATES, dateutil.compliment_datestring(str(date), -1, date_check))
        date = DATES[di-delay]

        qtrno = (int(date[:4]) - 2000) * 4 + np.floor((int(date[4:6])-1)/3) - quarter_offset - rtype
        if quarter is not None:
            query = 'date <= {0!r} & quarter == {1} & qtrno >= {2}'.format(date, quarter, qtrno)
        else:
            query = 'date <= {0!r} & quarter % {1} == 0 & qtrno >= {2}'.format(date, rtype, qtrno)
        df = JYFundFetcher.get_data(self.table, self._startyear).query(query)

        if dnames is not None:
            columns = ['sid', 'qtrno', 'date']
            for dname in dnames:
                if dname not in columns:
                    columns.append(dname)
            df = df.ix[:, columns]
        df.sort(['sid', 'qtrno', 'date'], inplace=True)

        res = {}
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        tmp = pool.imap_unordered(myfunc1, [(sid, sdf, quarter_offset) for sid, sdf in df.groupby('sid')])
        pool.close()
        pool.join()
        for x in tmp:
            res.update(x)
        df = pd.DataFrame(res).T[dnames]

        if reindex:
            return df.reindex(index=SIDS)
        return df

    def prepare_panel(self, dnames, startdate=None, enddate=None, rtype=QUARTER, use_adjust=None, fillna='ffill', **kwargs):
        """Prepare a Panel with item axis labled by data names.

        :param list dnames: List of data names
        :param startdate: Starting point of data. Default: None, defaults to the earliest date
        :param enddate: Ending point of data. Default: None, defaults to the last date
        :param enum rtype: What type of reports to be considered? QUARTER(1, default): all reports; SEMI(2): only semi-annual reports; YEAR(4): only annual reports
        :param use_adjust: How to treat data corrections? None(default): try to use all published data along date axis. False: use only first published data. True: use only last published data which are assumed to be public up to ``enddate``, thus may entail forward-looking bias in backtesting simulation
        :type use_adjust: None, boolean
        :param ffill: Fill gaps long date axis for those non-reporting days, currently supports ('ffill', None). Default: 'ffill'
        :type ffill: None, str
        """
        date_check = kwargs.get('date_check', self.date_check)
        reindex = kwargs.get('reindex', self.reindex)
        datetime_index = kwargs.get('datetime_index', self.datetime_index)

        if enddate is None:
            enddate = DATES[-1]
        else:
            enddate = dateutil.parse_date(DATES, dateutil.compliment_string(str(enddate), 1, date_check), -1)
        query = 'quarter % {0} == 0 & date <= {1!r}'.format(rtype, enddate)
        columns = ['sid', 'date', 'qtrno']
        for dname in dnames:
            if dname not in columns:
                columns.append(dname)
        df = JYFundFetcher.get_data(self.table, self._startyear)
        df = df.query(query).ix[:, columns]
        df.sort(['sid', 'date', 'qtrno'], inplace=True)

        res = {}
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        tmp = pool.map(myfunc2, [(sid, sdf, self.table, rtype, use_adjust) for sid, sdf in df.groupby('sid')])
        pool.close()
        pool.join()
        for x in tmp:
            res.update(x)
        panel = pd.Panel(res).transpose(1, 2, 0)

        if startdate is None:
            startdate = panel.major_axis[0]
        else:
            startdate = dateutil.parse_date(DATES, dateutil.compliment_string(str(enddate), -1, date_check), 1)
        si, ei = map(DATES.index, [startdate, enddate])
        panel = panel.reindex(major_axis=DATES[si: ei+1])

        if fillna is not None:
            panel = panel.fillna(axis=0, method='ffill')

        qtrno = pd.Series({date:
            (int(date[:4]) - 2000) * 4 + np.floor((int(date[4:6])-1.)/3) - rtype for date in panel.major_axis})
        valid = panel['qtrno'].ge(qtrno, axis=0)
        res = {}
        for dname in dnames:
            df = panel[dname]
            df[~valid] = np.nan
            res[dname] = df
        panel = pd.Panel(res)

        if datetime_index:
            panel.major_axis = pd.to_datetime(panel.major_axis)
            if reindex:
                panel = panel.reindex(minor_axis=SIDS, copy=False)
            return panel
        if reindex:
            return panel.reindex(minor_axis=SIDS, copy=False)
        return panel

    def fetch(self, *args, **kwargs):
        """Disabled.

        :raises: NotImplementedError
        """
        raise NotImplementedError

    def fetch_window(self, *args, **kwargs):
        """Disabled.

        :raises: NotImplementedError
        """
        raise NotImplementedError

    def fetch_history(self, dname, date, records=1, rtype=QUARTER, quarter=None, **kwargs):
        """Prepare cross-sectional data historic records into a DataFrame, using data with timestamps less than ``date`` minus ``delay``.

        :param int quarter_offset: Offset in quarter numbers. Default: 0
        :param enum rtype: What type of reports to be considered? QUARTER(1, default): all reports; SEMI(2): only semi-annual reports and annual reports; YEAR(4): only annual reports
        :param int quarter: It will only fetch data for this particular quarter(the 4 quarters in a year is 1, 2, 3, 4), thus should be compatible with the use of ``rtype``. Default: None, fetch whatever data is available

        .. seealso:: :py:meth:`JYFundFetcher.prepare_frame`
        """
        if quarter is not None:
            try:
                assert quarter % rtype == 0
            except AssertionError:
                raise ValueError('rtype {0!r} and quarter {1!r} are not compatible')

        date_check = kwargs.get('date_check', self.date_check)
        reindex = kwargs.get('reindex', self.reindex)
        delay = kwargs.get('delay', self.delay)

        di, date = dateutil.parse_date(DATES, dateutil.compliment_datestring(date, -1, date_check))
        date = DATES[di-delay]

        qtrno = (int(date[:4]) - 2000) * 4 + np.floor((int(date[4:6])-1)/3) - rtype * (records+1)
        if quarter is not None:
            query = 'date <= {0!r} & quarter == {1} & qtrno >= {2}'.format(date, quarter, qtrno)
        else:
            query = 'date <= {0!r} & quarter % {1} == 0 & qtrno >= {2}'.format(date, rtype, qtrno)

        columns = ['sid', 'qtrno', 'date', 'quarter', dname]
        df = JYFundFetcher.get_data(self.table, self._startyear)[columns].query(query)
        df.sort(['sid', 'qtrno', 'date'], inplace=True)

        res = {}
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        tmp = pool.imap_unordered(myfunc3, [(sid, sdf, records, rtype, dname, self.table) for sid, sdf in df.groupby('sid')])
        pool.close()
        pool.join()
        for x in tmp:
            res.update(x)
        df = pd.DataFrame(res)

        if reindex:
            df = df.reindex(columns=SIDS)
        if records == 1:
            return df.iloc[0]
        return df

    def fetch_daily(self, dname, date, offset=0, quarter_offset=0, rtype=QUARTER, quarter=None, **kwargs):
        """Return a series of cross-sectional data.

        :param int quarter_offset: Offset in quarter numbers. Default: 0
        :param enum rtype: Which type of reports to be considered
        :param int quarter: Should only used when ``rtype`` is QUARTER(1). It will only fetch data for this particular quarter, thus also making ``quarter_offset`` not meaningful. Default: None, fetch whatever data is available as of ``date-offset``
        """
        return self.prepare_frame(
                [dname],
                date=date,
                quarter_offset=quarter_offset,
                rtype=rtype,
                quarter=quarter,
                delay=offset,
                **kwargs)[dname]
