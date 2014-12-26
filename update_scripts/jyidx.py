"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from base import UpdaterBase


class JYIdxUpdater(UpdaterBase):
    """The updater class for collections 'jyidx'."""

    def __init__(self, timeout=30*60):
        super(JYIdxUpdater, self).__init__(timeout)

    def pre_update(self):
        self.collection = self.db.jyindex
        self.dates = self.db.dates.distinct('date')

    def pro_update(self):
        return

    @staticmethod
    def norm(df1, df0=None, col=None):
        if df0 is None:
            df1[col] = [0 if np.isnan(i) else i for i in df1[col]]
        else:
            df1[col] = [i if np.isnan(j) else j for i, j in zip(df0[col], df1[col])]

    def _update(self, date, df, ser, dname, dtype='Q'):
        df = pd.concat([ser, df[['sid', 'qtrno']]], axis=1)
        for sid, val in ser.iteritems():
            key = {'date': date, 'sid': sid, 'dtype': dtype}
            doc = {dname: val, 'qtrno': df['qtrno'][sid]}
            self.collection.update(key, {'$set': doc}, upsert=True)

    def update(self, date):
        date = self.dates[self.dates.index(date)-1]
        cursor = self.db.jydata.find({'date': date}, {'_id': 0, 'date': 0})
        self.data = pd.DataFrame(list(cursor))
        if len(self.data) == 0:
            self.logger.warning('No records found for {} on {}', self.collection.name, date)
            return
        self.Q0 = self.data.query('dtype == "Q0"')
        self.Q0.index = self.Q0.sid
        self.Q1 = self.data.query('dtype == "Q-1"')
        if len(self.Q1) == 0:
            self.Q1 = self.Q0.copy()
        self.Q1.index = self.Q1.sid
        self.Q1 = self.Q1.reindex(index=self.Q0.index)

        self.S0 = self.data.query('dtype == "S0"')
        self.S0.index = self.S0.sid
        self.S1 = self.data.query('dtype == "S-1"')
        if len(self.S1) == 0:
            self.S1 = self.S0.copy()
        self.S1.index = self.S1.sid
        self.S1 = self.S1.reindex(index=self.S0.index)

        self.Y0 = self.data.query('dtype == "Y0"')
        self.Y0.index = self.Y0.sid
        self.Y1 = self.data.query('dtype == "Y-1"')
        if len(self.Y1) == 0:
            self.Y1 = self.Y0.copy()
        self.Y1.index = self.Y1.sid
        self.Y1 = self.Y1.reindex(index=self.Y0.index)

        self.TTM = self.data.query('dtype == "TTM"')
        self.TTM.index = self.TTM.sid

        self.update_roe(date)

    def update_roe(self, date):
        roe = self.TTM['np_shareholder'] / self.TTM['se_without_mi']
        self._update(date, self.TTM, roe, 'roe', 'TTM')

        self.norm(self.Q1, self.Q0, 'se_without_mi')
        se_without_mi = (self.Q0['se_without_mi']+self.Q1['se_without_mi']) * 0.5
        roe = self.Q0['np_shareholder'] /  se_without_mi
        self._update(date, self.Q0, roe, 'roe', 'Q')

        if len(self.SO):
            self.norm(self.S1, self.S0, 'se_without_mi')
            se_without_mi = (self.S0['se_without_mi']+self.S1['se_without_mi']) * 0.5
            roe = self.S0['np_shareholder'] /  se_without_mi
            self._update(date, self.S0, roe, 'roe', 'S')

        if len(self.Y0):
            self.norm(self.Y1, self.Y0, 'se_without_mi')
            se_without_mi = (self.Y0['se_without_mi']+self.Y1['se_without_mi']) * 0.5
            roe = self.Y0['np_shareholder'] /  se_without_mi
            self._update(date, self.Y0, roe, 'roe', 'Y')


    def update_roe_cut(self, date):
        roe_cut = self.TTM['nps_cut'] / self.TTM['se_without_mi']
        self._update(date, self.TTM, roe_cut, 'roe_cut', 'TTM')

        self.norm(self.Q1, self.Q0, 'se_without_mi')
        se_without_mi = (self.Q0['se_without_mi']+self.Q1['se_without_mi']) * 0.5
        roe_cut = self.Q0['nps_cut'] /  se_without_mi
        self._update(date, self.Q0, roe_cut, 'roe_cut')

        if len(self.SO):
            self.norm(self.S1, self.S0, 'se_without_mi')
            se_without_mi = (self.S0['se_without_mi']+self.S1['se_without_mi']) * 0.5
            roe_cut = self.S0['nps_cut'] /  se_without_mi
            self._update(date, self.S0, roe_cut, 'roe_cut', 'S')

        if len(self.Y0):
            self.norm(self.Y1, self.Y0, 'se_without_mi')
            se_without_mi = (self.Y0['se_without_mi']+self.Y1['se_without_mi']) * 0.5
            roe_cut = self.Y0['nps_cut'] /  se_without_mi
            self._update(date, self.Y0, roe_cut, 'roe_cut', 'Y')

    def update_roa(self, date):
        roa = self.TTM['np_shareholder'] / self.TTM['assets']
        self._update(date, self.TTM, roa, 'roa', 'TTM')

        self.norm(self.Q1, self.Q0, 'assets')
        assets = (self.Q0['assets']+self.Q1['assets']) * 0.5
        roa = self.Q0['np_shareholder'] / assets
        self._update(date, self.Q0, roa, 'roa')

        if len(self.SO):
            self.norm(self.S1, self.S0, 'assets')
            assets = (self.S0['assets']+self.S1['assets']) * 0.5
            roa = self.S0['np_shareholder'] /  assets
            self._update(date, self.S0, roa, 'roa', 'S')

        if len(self.Y0):
            self.norm(self.Y1, self.Y0, 'assets')
            assets = (self.Y0['assets']+self.Y1['assets']) * 0.5
            roa = self.Y0['np_shareholder'] /  assets
            self._update(date, self.Y0, roa, 'roa', 'Y')

    def update_roic(self, date):
        roic = (self.TTM['ebit'] * (1 - self.TTM['income_tax_expense'] / self.TTM['total_profit'])) / self.TTM['invested_capital']

        self.norm(self.Q1, self.Q0, 'invested_capital')
        invested_capital = (self.Q0['invested_capital']+self.Q1['invested_capital']) * 0.5
        roic = (self.Q0['ebit'] * (1 - self.Q0['income_tax_expense'] / self.Q0['total_profit'])) / invested_capital
        self._update(date, self.Q0, roic, 'roic')

        if len(self.S0):
            self.norm(self.S1, self.S0, 'invested_capital')
            invested_capital = (self.S0['invested_capital']+self.S1['invested_capital']) * 0.5
            roic = (self.S0['ebit'] * (1 - self.S0['income_tax_expense'] / self.S0['total_profit'])) / invested_capital
            self._update(date, self.S0, roic, 'roic')

        if len(self.Y0):
            self.norm(self.Y1, self.Y0, 'invested_capital')
            invested_capital = (self.Y0['invested_capital']+self.Y1['invested_capital']) * 0.5
            roic = (self.Y0['ebit'] * (1 - self.Y0['income_tax_expense'] / self.Y0['total_profit'])) / invested_capital
            self._update(date, self.Y0, roic, 'roic')


if __name__ == '__main__':
    jyidx = JYIdxUpdater()
    jyidx.run()
