"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from base import UpdaterBase


class JYDerivedUpdater(UpdaterBase):
    """The updater class for collections 'jydata'."""

    def __init__(self, timeout=30*60):
        super(JYDerivedUpdater, self).__init__(timeout)

    def pre_update(self):
        self.collection = self.db.jydata
        self.dates = self.db.dates.distinct('date')

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dtype_1_date_1_sid_1 on collection {}', self.collection.name)
        self.collection.ensure_index([('dtype', 1), ('date', 1), ('sid', 1)], background = True)

    @staticmethod
    def norm(flt):
        return 0 if np.isnan(flt) else flt

    def update(self, date):
        date = self.dates[self.dates.index(date)-1]
        sids = set()
        cursor = self.db.jybs.find({'date': date}, {'_id': 0, 'sid': 1})
        for row in cursor:
            sids.add(row['sid'])
        cursor = self.db.jycs.find({'date': date}, {'_id': 0, 'sid': 1})
        for row in cursor:
            sids.add(row['sid'])
        cursor = self.db.jyis.find({'date': date}, {'_id': 0, 'sid': 1})
        for row in cursor:
            sids.add(row['sid'])
        if not sids:
            return
        self.update_jybs(date, sids)
        self.update_jycs(date, sids)
        self.update_jyis(date, sids)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', len(sids), self.collection.name, self.db.name, date)

    def update_jyis(self, date, sids):
        dnames = [
'total_operating_income', 'operating_income', 'net_interest_income', 'interest_income', 'interest_expense',
'total_operating_expense', 'operating_payout', 'operating_expense', 'selling_expense', 'administration_expense', 'financial_expense',
'nonrecurring_pnl', 'nonoperating_income', 'nonoperating_expense', 'noncurrent_assets_deal_loss',
'operating_profit', 'total_profit', 'income_tax_expense', 'net_profit', 'np_shareholder',
]
        cursor = self.db.jyis.find({'date': {'$lte': date}, 'sid': {'$in': list(sids)}}, {'_id': 0})
        df = pd.DerivedFrame(list(cursor))
        if len(df) == 0:
            return
        dnames = [dname for dname in dnames if dname in df.columns]
        df['qtrno'] = 4*(df.year-2000) + df.quarter
        for sid, sdf in df.groupby('sid'):
            df = sdf[dnames+['qtrno', 'date', 'year']].sort(['qtrno', 'date']).drop_duplicates('qtrno', take_last=True)
            df.index = df.qtrno
            # dtype: 'Y'
            df1 = df.ix[df.qtrno % 4 == 0].copy()
            if not len(df1):
                continue
            df1.year -= df1.year.iloc[-1]
            for i, y in df1.year.iteritems():
                if y <= -2:
                    continue
                key = {'date': date, 'sid': sid, 'dtype': 'Y'+str(y)}
                doc = {dname: df1[dname][i] for dname in dnames}
                doc.update({'qtrno': df1.qtrno.iloc[-1]})
                self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'S'
            df2 = df.ix[df.qtrno % 2 == 0].copy()
            if not len(df2):
                continue
            cs = df2.qtrno.iloc[-1]
            for i in range(len(df2)-1, -1, -1):
                s = df2.qtrno.iloc[i]
                if (s-cs)/2 <= -4:
                    continue
                if s % 4 == 2:
                    key = {'date': date, 'sid': sid, 'dtype': 'S'+str((s-cs)/2)}
                    doc = {dname: df2[dname][s] for dname in dnames}
                    doc.update({'qtrno': df2.qtrno.iloc[-1]})
                    self.collection.update(key, {'$set': doc}, upsert=True)
                else:
                    ps = s-2
                    if ps in df2.qtrno:
                        key = {'date': date, 'sid': sid, 'dtype': 'S'+str((s-cs)/2)}
                        doc = {dname: df2[dname][s]-self.norm(df2[dname][ps]) for dname in dnames}
                        doc.update({'qtrno': df2.qtrno.iloc[-1]})
                        self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'Q'
            df3 = df.copy()
            if not len(df3):
                continue
            cq = df3.qtrno.iloc[-1]
            for i in range(len(df3)-1, -1, -1):
                q = df3.qtrno.iloc[i]
                if q-cq <= -8:
                    continue
                if q % 4 == 1:
                    key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                    doc = {dname: df3[dname][s] for dname in dnames}
                    doc.update({'qtrno': df3.qtrno.iloc[-1]})
                    self.collection.update(key, {'$set': doc}, upsert=True)
                else:
                    pq = q-1
                    if pq in df3.qtrno:
                        key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                        doc = {dname: df3[dname][q]-self.norm(df3[dname][pq]) for dname in dnames}
                        doc.update({'qtrno': df3.qtrno.iloc[-1]})
                        self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'TTM'
            df4 = df.copy()
            if not len(df4):
                continue
            cq = df4.qtrno.iloc[-1]
            if cq % 4 == 0:
                key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                doc = {dname: df4[dname][cq] for dname in dnames}
                doc.update({'qtrno': df4.qtrno.iloc[-1]})
                self.collection.update(key, {'$set': doc}, upsert=True)
            else:
                pyq, py = cq - 4, cq - cq % 4
                if py in df4.qtrno:
                    if pyq not in df4.qtrno:
                        key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                        doc = {dname: df4[dname][py] for dname in dnames}
                        doc.update({'qtrno': df4.qtrno.iloc[-1]})
                        self.collection.update(key, {'$set': doc}, upsert=True)
                    else:
                        key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                        doc = {dname: self.norm(df4[dname][py])-self.norm(df4[dname][pyq])+df4[dname][cq] for dname in dnames}
                        doc.update({'qtrno': df4.qtrno.iloc[-1]})
                        self.collection.update(key, {'$set': doc}, upsert=True)

    def update_jycs(self, date, sids):
        dnames = [
'goods_service_cash_in', 'tax_refund_cash_in', 'ocif',
'goods_service_cash_out', 'staff_paid_cash_out', 'tax_paid_cash_out', 'ocof', 'ocf',
'icif', 'icof', 'icf', 'fcif', 'fcof', 'fcf',
'cae_increase', 'fixed_assets_depreciation', 'intangible_assets_amortization'
]
        cursor = self.db.jycs.find({'date': {'$lte': date}, 'sid': {'$in': list(sids)}}, {'_id': 0})
        df = pd.DerivedFrame(list(cursor))
        if len(df) == 0:
            return
        dnames = [dname for dname in dnames if dname in df.columns]
        df['qtrno'] = 4*(df.year-2000) + df.quarter
        for sid, sdf in df.groupby('sid'):
            df = sdf[dnames+['qtrno', 'date', 'year']].sort(['qtrno', 'date']).drop_duplicates('qtrno', take_last=True)
            df.index = df.qtrno
            # dtype: 'Y'
            df1 = df.ix[df.qtrno % 4 == 0].copy()
            if not len(df1):
                continue
            df1.year -= df1.year.iloc[-1]
            for i, y in df1.year.iteritems():
                if y <= -2:
                    continue
                key = {'date': date, 'sid': sid, 'dtype': 'Y'+str(y)}
                doc = {dname: df1[dname][i] for dname in dnames}
                doc.update({'qtrno': df1.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'S'
            df2 = df.ix[df.qtrno % 2 == 0].copy()
            if not len(df2):
                continue
            cs = df2.qtrno.iloc[-1]
            for i in range(len(df2)-1, -1, -1):
                s = df2.qtrno.iloc[i]
                if (s-cs)/2 <= -4:
                    continue
                if s % 4 == 2:
                    key = {'date': date, 'sid': sid, 'dtype': 'S'+str((s-cs)/2)}
                    doc = {dname: df2[dname][s] for dname in dnames}
                    doc.update({'qtrno': df2.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                    self.collection.update(key, {'$set': doc}, upsert=True)
                else:
                    ps = s-1
                    if ps in df2.qtrno:
                        key = {'date': date, 'sid': sid, 'dtype': 'S'+str((s-cs)/2)}
                        doc = {dname: df2[dname][s]-self.norm(df2[dname][ps]) for dname in dnames}
                        doc.update({'qtrno': df2.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                        self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'Q'
            df3 = df.copy()
            if not len(df3):
                continue
            cq = df3.qtrno.iloc[-1]
            for i in range(len(df3)-1, -1, -1):
                q = df3.qtrno.iloc[i]
                if q-cq <= -8:
                    continue
                if q % 4 == 1:
                    key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                    doc = {dname: df3[dname][s] for dname in dnames}
                    doc.update({'qtrno': df3.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                    self.collection.update(key, {'$set': doc}, upsert=True)
                else:
                    pq = q-1
                    if pq in df3.qtrno:
                        key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                        doc = {dname: df3[dname][q]-self.norm(df3[dname][pq]) for dname in dnames}
                        doc.update({'qtrno': df3.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                        self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'TTM'
            df4 = df.copy()
            if not len(df4):
                continue
            cq = df4.qtrno.iloc[-1]
            if cq % 4 == 0:
                key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                doc = {dname: df4[dname][cq] for dname in dnames}
                doc.update({'qtrno': df4.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                self.collection.update(key, {'$set': doc}, upsert=True)
            else:
                pyq, py = cq - 4, cq - cq % 4
                if py in df4.qtrno:
                    if pyq not in df4.qtrno:
                        key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                        doc = {dname: df4[dname][py] for dname in dnames}
                        doc.update({'qtrno': df4.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                        self.collection.update(key, {'$set': doc}, upsert=True)
                    else:
                        key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                        doc = {dname: self.norm(df4[dname][py])-self.norm(df4[dname][pyq])+df4[dname][cq] for dname in dnames}
                        doc.update({'qtrno': df4.qtrno.iloc[-1], 'depreciation_amortization': doc['fixed_assets_depreciation']+doc['intangible_assets_amortization']})
                        self.collection.update(key, {'$set': doc}, upsert=True)

    def update_jybs(self, date, sids):
        dnames = [
'cash_and_equivalent', 'trading_assets', 'note_receivable', 'dividend_receivable', 'interest_receivable', 'account_receivable', 'other_receivable', 'advance_payment', 'inventory', 'deferred_expense', 'noncurrent_assets_in_1y', 'current_assets',
'held_for_sale_assets', 'held_to_maturity_investment', 'investment_property', 'longterm_equity_investment', 'longterm_receivable_account', 'fixed_assets', 'intangible_assets', 'research_and_development', 'goodwill', 'long_deferred_expense', 'deferred_tax_assets', 'noncurrent_assets', 'total_assets',
'shortterm_loan', 'impawned_loan', 'trading_liability', 'note_payable', 'account_payable', 'shortterm_bond_payable', 'advance_receipt', 'salary_payable', 'dividend_payable', 'tax_payable', 'interest_payable', 'accrued_expense', 'deferred_proceed', 'noncurrent_liability_in_1y', 'current_liability',
'longterm_loan', 'bond_payable', 'longterm_account_payable', 'specific_account_payable', 'estimate_liability', 'deferred_tax_liability', 'noncurrent_liability', 'liability',
'paidin_capital', 'capital_reserve_fund', 'surplus_reserve_fund', 'retained_earnings', 'se_without_mi', 'minority_interest', 'shareholder_equity', 'liability_and_equity'
]
        cursor = self.db.jybs.find({'date': {'$lte': date}, 'sid': {'$in': list(sids)}}, {'_id': 0})
        df = pd.DerivedFrame(list(cursor))
        if len(df) == 0:
            return
        dnames = [dname for dname in dnames if dname in df.columns]
        df['qtrno'] = 4*(df.year-2000) + df.quarter
        for sid, sdf in df.groupby('sid'):
            df = sdf[dnames+['qtrno', 'date', 'year']].sort(['qtrno', 'date']).drop_duplicates('qtrno', take_last=True)
            df.index = df.qtrno
            # dtype: 'Y'
            df1 = df.ix[df.qtrno % 4 == 0].copy()
            if not len(df1):
                continue
            df1.year -= df1.year.iloc[-1]
            for i, y in df1.year.iteritems():
                if y <= -2:
                    continue
                key = {'date': date, 'sid': sid, 'dtype': 'Y'+str(y)}
                doc = {dname: df1[dname][i] for dname in dnames}
                doc.update({'qtrno': df1.qtrno.iloc[-1]})
                self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'S'
            df2 = df.ix[df.qtrno % 2 == 0].copy()
            if not len(df2):
                continue
            cs = df2.qtrno.iloc[-1]
            for i in range(len(df2)-1, -1, -1):
                s = df2.qtrno.iloc[i]
                if (s-cs)/2 <= -4:
                    continue
                key = {'date': date, 'sid': sid, 'dtype': 'S'+str((s-cs)/2)}
                doc = {dname: df2[dname][s] for dname in dnames}
                doc.update({'qtrno': df2.qtrno.iloc[-1]})
                self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'Q'
            df3 = df.copy()
            if not len(df3):
                continue
            cq = df3.qtrno.iloc[-1]
            for i in range(len(df3)-1, -1, -1):
                q = df3.qtrno.iloc[i]
                if q-cq <= -8:
                    continue
                key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                doc = {dname: df3[dname][s] for dname in dnames}
                doc.update({'qtrno': df3.qtrno.iloc[-1]})
                self.collection.update(key, {'$set': doc}, upsert=True)


if __name__ == '__main__':
    jyder = JYDerivedUpdater()
    jyder.run()
