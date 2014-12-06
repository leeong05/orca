"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import pandas as pd
import logbook
logbook.set_datetime_format('local')

from engine import fetch_sql


class AlphaDBFetcher(object):
    """Base class to fetch data from the alpha DB.

    .. note::

       This is a base class and should not be used directly.
    """

    LOGGER_NAME = 'alphadb'

    def __init__(self, **kwargs):
        self.logger = logbook.Logger(AlphaDBFetcher.LOGGER_NAME)
        self.delay = kwargs.get('delay', 0)

    def debug(self, msg):
        """Logs a message with level DEBUG on the alpha logger."""
        self.logger.debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the alpha logger."""
        self.logger.info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the alpha logger."""
        self.logger.warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the alpha logger."""
        self.logger.error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the alpha logger."""
        self.logger.critical(msg)


class AlphaInfoFetcher(AlphaDBFetcher):
    """Class to fetch alpha info from the alpha DB."""

    FETCH = (
            "SELECT"
            "  info.id AS id, info.name AS name, author, market, uinfo.name AS universe, category, status, proddate, entrydate, enddate "
            "FROM"
            "  info JOIN uinfo ON info.uid = uinfo.id "
            )

    def fetch(self, **kwargs):
        """Fetch alpha ids by some criteria, for example, status, market, author etc."""
        sql = AlphaInfoFetcher.FETCH
        criteria = []
        status = kwargs.get('status', None)
        if status:
            criteria.append("  status = {0!r}".format(status))
        category = kwargs.get('category', None)
        if category:
            criteria.append("  category = {0!r}".format(category))
        market = kwargs.get('market', None)
        if market:
            criteria.append("  market = {0!r}".format(market))
        author = kwargs.get('author', None)
        if author:
            criteria.append("  author LIKE '%{0:s}%'".format(author))
        if criteria:
            sql = sql + "WHERE" + "  AND".join(criteria)
        print sql
        res = fetch_sql(sql)
        query = kwargs.get('query', None)
        if query:
            return res.query(query)
        return res


class ScoreFetcher(AlphaDBFetcher):
    """Class to fetch scores of alphas from the alpha DB."""

    FETCH_IDS = (
            "SELECT"
            "  id, date, sid, score "
            "FROM"
            "  score "
            "WHERE"
            "  id IN ({0:s})"
            "  AND"
            "  date >= STR_TO_DATE({1:s}, '%Y%m%d')")
    FETCH_IDS_ENDDATE = (
            "SELECT"
            "  id, date, sid, score "
            "FROM"
            "  score "
            "WHERE"
            "  id IN ({0:s})"
            "  AND"
            "  date >= STR_TO_DATE({1:s}, '%Y%m%d')"
            "  AND"
            "  date <= STR_TO_DATE({2:s}, '%Y%m%d')")

    def fetch(self, ids, startdate, enddate=None):
        """
        :param ids: Alpha id numbers
        :type ids: int, list
        """
        if isinstance(ids, int):
            ids_ = [ids]
        else:
            ids_ = ids
        sql = ScoreFetcher.FETCH_ID.format(', '.join([str(i) for i in ids_]), startdate) if enddate is None else \
              ScoreFetcher.FETCH_ID_ENDDATE.format(', '.join([str(i) for i in ids_]), startdate, enddate)
        res = fetch_sql(sql)
        panel = {}
        for id_, df in res.groupby('id'):
            panel[id_] = df.pivot('date', 'sid', 'score')
        if not panel:
            return None

        panel = pd.Panel(panel)
        if isinstance(ids, int):
            return panel[ids]
        return panel

    def fetch_window(self, ids, window):
        return self.fetch(ids, window[0], window[-1])

    def fetch_history(self, ids, date, backdays, delay=None):
        if delay is None:
            delay = self.delay
        di, date = self.parse_date(self.dates, date, -1)
        return self.fetch_window(ids, self.dates[di-delay-backdays+1: di-delay+1])

    def fetch_daily(self, ids, date, offset=0):
        if isinstance(ids, int):
            return self.fetch_history(ids, date, 1, delay=offset).iloc[0]
        panel = self.fetch_history(ids, date, 1, delay=offset)
        return panel.major_xs(panel.major_axis[0])


class UniverseFetcher(AlphaDBFetcher):
    """Class to fetch universes from the alpha DB."""

    FETCH_NAME = (
            "SELECT"
            "  date, sid, valid "
            "FROM"
            "  universe JOIN uinfo USING(id) "
            "WHERE"
            "  uinfo.name = {0!r}"
            "  AND"
            "  date >= STR_TO_DATE({1:s}, '%Y%m%d')")
    FETCH_NAME_ENDDATE = (
            "SELECT"
            "  date, sid, valid "
            "FROM"
            "  universe JOIN uinfo USING(id) "
            "WHERE"
            "  uinfo.name = {0!r}"
            "  AND"
            "  date >= STR_TO_DATE({1:s}, '%Y%m%d')"
            "  AND"
            "  date <= STR_TO_DATE({1:s}, '%Y%m%d')")

    def fetch(self, uname, startdate, enddate=None):
        """
        :param str uname: Universe name
        """
        sql = UniverseFetcher.FETCH_NAME.format(uname, startdate) if enddate is None else \
              UniverseFetcher.FETCH_NAME_ENDDATE.format(uname, startdate, enddate)
        return fetch_sql(sql).pivot('date', 'sid', 'valid').fillna(False).astype(bool)

    def fetch_window(self, uname, window):
        return self.fetch(uname, window[0], window[-1])

    def fetch_history(self, uname, date, backdays, delay=None):
        if delay is None:
            delay = self.delay
        di, date = self.parse_date(self.dates, date, -1)
        return self.fetch_window(uname, self.dates[di-delay-backdays+1: di-delay+1])

    def fetch_daily(self, uname, date, offset=0):
        return self.fetch_history(uname, date, 1, delay=offset).iloc[0]


class PerformanceFetcher(AlphaDBFetcher):
    """Class to fetch performance of alphas from the alpha DB."""

    FETCH_DATA_IDS = (
            "SELECT"
            "  aid, date, {0:s} "
            "FROM"
            "  performance JOIN info ON performance.aid = info.id AND performance.uid = info.uid "
            "WHERE"
            "  id IN ({1:s})"
            "  AND"
            "  date >= STR_TO_DATE({2:s}, '%Y%m%d')")
    FETCH_DATA_IDS_ENDDATE = (
            "SELECT"
            "  id, date, {0:s} "
            "FROM"
            "  performance JOIN info ON performance.aid = info.id AND performance.uid = info.uid "
            "WHERE"
            "  id IN ({1:s})"
            "  AND"
            "  date >= STR_TO_DATE({2:s}, '%Y%m%d')"
            "  AND"
            "  date <= STR_TO_DATE({3:s}, '%Y%m%d')")
    FETCH_DATA_IDS_UNIVERSE = (
            "SELECT"
            "  id, date, {0:s} "
            "FROM"
            "  performance JOIN uinfo ON performance.uid = uinfo.id "
            "WHERE"
            "  uinfo.name = {1!r}"
            "  AND"
            "  id IN ({2:s})"
            "  AND"
            "  date >= STR_TO_DATE({3:s}, '%Y%m%d')")
    FETCH_DATA_IDS_UNIVERSE_ENDDATE = (
            "SELECT"
            "  id, date, {0:s} "
            "FROM"
            "  performance JOIN uinfo ON performance.uid = uinfo.id "
            "WHERE"
            "  uinfo.name = {1!r}"
            "  AND"
            "  id IN ({2:s})"
            "  AND"
            "  date >= STR_TO_DATE({3:s}, '%Y%m%d')"
            "  AND"
            "  date <= STR_TO_DATE({4:s}, '%Y%m%d')")
    FETCH_DATAS_ID = (
            "SELECT"
            "  date, {0:s} "
            "FROM"
            "  performance JOIN info ON performance.aid = info.id AND performance.uid = info.uid "
            "WHERE"
            "  id = {1:d}"
            "  AND"
            "  date >= STR_TO_DATE({2:s}, '%Y%m%d')")
    FETCH_DATAS_ID_ENDDATE = (
            "SELECT"
            "  date, {0:s} "
            "FROM"
            "  performance JOIN info ON performance.aid = info.id AND performance.uid = info.uid "
            "WHERE"
            "  id = {1:d}"
            "  AND"
            "  date >= STR_TO_DATE({2:s}, '%Y%m%d')"
            "  AND"
            "  date <= STR_TO_DATE({3:s}, '%Y%m%d')")
    FETCH_DATAS_ID_UNIVERSE = (
            "SELECT"
            "  date, {0:s} "
            "FROM"
            "  performance JOIN uinfo ON performance.uid = uinfo.id "
            "WHERE"
            "  id = {1:d}"
            "  AND"
            "  uinfo.name = {2!r}"
            "  AND"
            "  date >= STR_TO_DATE({3:s}, '%Y%m%d')")
    FETCH_DATAS_ID_UNIVERSE_ENDDATE = (
            "SELECT"
            "  date, {0:s} "
            "FROM"
            "  performance JOIN uinfo ON performance.uid = uinfo.id "
            "WHERE"
            "  id = {1:d}"
            "  AND"
            "  uinfo.name = {2!r}"
            "  AND"
            "  date >= STR_TO_DATE({3:s}, '%Y%m%d')"
            "  AND"
            "  date <= STR_TO_DATE({4:s}, '%Y%m%d')")

    def fetch(self, datas, startdate, enddate=None, **kwargs):
        """
        :param datas: Names of the performance metrics, for example: 'returns'
        :type datas: str, list
        :raises ValueError: When ``datas`` is a list, ``id`` in ``kwargs`` can only a single alpha id(i.e. an integer)
        """
        ids = kwargs.pop('id')
        str_ids = ids if isinstance(ids, int) else ', '.join([str(i) for i in ids])
        uname = kwargs.get('universe', None)
        if isinstance(datas, str):
            if uname is None:
                sql = PerformanceFetcher.FETCH_DATA_IDS.format(datas, str_ids, startdate) if enddate is None else \
                      PerformanceFetcher.FETCH_DATA_IDS.format(datas, str_ids, startdate, enddate)
            else:
                sql = PerformanceFetcher.FETCH_DATA_IDS_UNIVERSE.format(datas, uname, str_ids, startdate) if enddate is None else \
                      PerformanceFetcher.FETCH_DATA_IDS_UNIVERSE_ENDDATE.format(datas, uname, str_ids, startdate, enddate)
            res = fetch_sql(sql)
            df = {}
            for id_, sdf in res.groupby('id'):
                sdf.index = sdf.date
                df[id_] = sdf[datas]
            if not df:
                return None

            df = pd.DataFrame(df)
            if isinstance(ids, int):
                return df[ids]
            return df

        if not isinstance(ids, int):
            raise ValueError('Unsupported value combination with datas and ids both as list')

        str_datas = ', '.join(datas)
        if uname is None:
            sql = PerformanceFetcher.FETCH_DATAS_ID.format(str_datas, ids, startdate) if enddate is None else \
                  PerformanceFetcher.FETCH_DATAS_ID_ENDDATE.format(str_datas, ids, startdate, enddate)
        else:
            sql = PerformanceFetcher.FETCH_DATAS_ID_UNIVERSE.format(str_datas, ids, startdate) if enddate is None else \
                  PerformanceFetcher.FETCH_DATAS_ID_UNIVERSE_ENDDATE.format(str_datas, ids, startdate, enddate)
        df = fetch_sql(sql)
        df.index = df.date
        del df.date
        return df
