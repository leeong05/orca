"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from base import UpdaterBase


class JYDataUpdater(UpdaterBase):
    """The updater class for collections 'jydata'."""

    def __init__(self, timeout=30*60):
        super(JYDataUpdater, self).__init__(timeout)

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

    @staticmethod
    def add_col(df, col):
        if col not in df.columns:
            df[col] = np.nan

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
        self.update_composite(date, sids)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', len(sids), self.collection.name, self.db.name, date)

    def update_composite(self, date, sids):
        cursor = self.collection.find({'date': {'$lte': date, '$gte': self.dates[self.dates.index(date)-120]}, 'sid': {'$in': list(sids)}}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        res = []
        for (sid, dtype), sdf in df.groupby(['sid', 'dtype']):
            sdf = sdf[sdf.qtrno == sdf.qtrno.max()]
            sdf = sdf.sort('date').fillna(method='ffill')
            if sdf.date.max() == date:
                res.append(sdf.iloc[-1])
        df = pd.concat(res, axis=1).T

        df['tax_rate'] = df['income_tax_expense'] / df['total_profit']
        df['ebitda'] = [i+self.norm(j) for i, j in zip(df['ebit'], df['da'])]
        df['capex'] = [self.norm(i+j) for i, j in zip(df['delta_fixed_assets'], df['fixed_assets_depreciation'])]
        df['efcf'] = [i1 + (self.norm(i2) if np.isnan(i3) else -1*i3)*(1- i4) - (i5 + self.norm(i6)) for i1, i2, i3, i4, i5, i6 in zip(df['ebitda'],df['financial_expense'], df['net_interest_income'], df['tax_rate'], df['capex'], df['delta_working_capital'])]
        df['efcf'] = [i1 + i2 - (i3+self.norm(i4)) + i5  for i1, i2, i3, i4, i5 in zip(df['np_shareholder'],df['da'], df['capex'], df['delta_working_capital'], df['delta_net_borrowing'])]
        df['accruals_bs'] = df['delta_net_operating_assets']
        df['accruals_cf'] = df['net_profit'] - (df['ocf']+df['icf'])
        for _, row in df.iterrows():
            key = {'date': row['date'], 'sid': row['sid'], 'dtype': row['dtype']}
            doc = row.to_dict()
            del doc['date'], doc['sid'], doc['dtype']
            self.collection.update(key, {'$set': doc}, upsert=True)

    def update_jyis(self, date, sids):
        dnames = [
'total_operating_income', 'operating_income', 'net_interest_income', 'interest_income', 'interest_expense',
'total_operating_expense', 'operating_payout', 'operating_expense', 'selling_expense', 'administration_expense', 'financial_expense',
'nonrecurring_pnl', 'nonoperating_income', 'nonoperating_expense', 'noncurrent_assets_deal_loss',
'operating_profit', 'total_profit', 'income_tax_expense', 'net_profit', 'np_shareholder', 'other_comprehensive_income', 'tci_shareholder', 'nps_cut', 'ebit', 'ebi', 'ebt',
]
        cursor = self.db.jyis.find({'date': {'$lte': date}, 'sid': {'$in': list(sids)}}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        if len(df) == 0:
            return
        for dname in dnames:
            self.add_col(df, dname)
        df['tax_rate'] = df['income_tax_expense'] / df['total_profit']
        df['np_cut'] = [i-self.norm(j)*(1-self.norm(k)) for i, j, k in zip(df['net_profit'], df['nonrecurring_pnl'], df['tax_rate'])]
        df['nps_cut'] = df['np_shareholder'] / df['net_profit'] * df['np_cut']
        df['ebit'] = [i + self.norm(j) if np.isnan(k) else -1*k for i, j, k in zip(df['total_profit'],df['financial_expense'], df['net_interest_income'])]
        df['ebi'] = df['ebit'] - df['income_tax_expense']
        df['ebt'] = df['total_profit']
        dnames = [dname for dname in dnames if dname in df.columns]
        df['qtrno'] = 4*(df.year-2000) + df.quarter
        for sid, sdf in df.groupby('sid'):
            df = sdf[dnames+['qtrno', 'date', 'year']].sort(['qtrno', 'date']).drop_duplicates('qtrno', take_last=True)
            df.index = df.qtrno
            # dtype: 'Y'
            df1 = df.ix[df.qtrno % 4 == 0].copy()
            if len(df1):
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
            if len(df2):
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
            if len(df3):
                cq = df3.qtrno.iloc[-1]
                for i in range(len(df3)-1, -1, -1):
                    q = df3.qtrno.iloc[i]
                    if q-cq <= -8:
                        continue
                    if q % 4 == 1:
                        key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                        doc = {dname: df3[dname][q] for dname in dnames}
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
            if len(df4):
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
'icif', 'icof', 'icf', 'fcif', 'fcof', 'fcf', 'cae_increase', 'fixed_assets_depreciation', 'da'
]
        cursor = self.db.jycs.find({'date': {'$lte': date}, 'sid': {'$in': list(sids)}}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        if len(df) == 0:
            return
        for dname in dnames:
            self.add_col(df, dname)
        self.add_col(df, 'intangible_assets_amortization')
        self.add_col(df, 'deferred_expense_amortization')
        df['da'] = [np.nan if np.isnan(i) and np.isnan(j) and np.isnan(k) else self.norm(i)+self.norm(j)+self.norm(k) for i, j, k in zip(df['fixed_assets_depreciation'], df['intangible_assets_amortization'], df['deferred_expense_amortization'])]
        dnames = [dname for dname in dnames if dname in df.columns]
        df['qtrno'] = 4*(df.year-2000) + df.quarter
        for sid, sdf in df.groupby('sid'):
            df = sdf[dnames+['qtrno', 'date', 'year']].sort(['qtrno', 'date']).drop_duplicates('qtrno', take_last=True)
            df.index = df.qtrno
            # dtype: 'Y'
            df1 = df.ix[df.qtrno % 4 == 0].copy()
            if len(df1):
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
            if len(df2):
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
                        ps = s-1
                        if ps in df2.qtrno:
                            key = {'date': date, 'sid': sid, 'dtype': 'S'+str((s-cs)/2)}
                            doc = {dname: df2[dname][s]-self.norm(df2[dname][ps]) for dname in dnames}
                            doc.update({'qtrno': df2.qtrno.iloc[-1]})
                            self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'Q'
            df3 = df.copy()
            if len(df3):
                cq = df3.qtrno.iloc[-1]
                for i in range(len(df3)-1, -1, -1):
                    q = df3.qtrno.iloc[i]
                    if q-cq <= -8:
                        continue
                    if q % 4 == 1:
                        key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                        doc = {dname: df3[dname][q] for dname in dnames}
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
            if len(df4):
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

    def update_jybs(self, date, sids):
        dnames = [
'cash_and_equivalent', 'trading_assets', 'account_receivable', 'inventory', 'current_assets',
'held_for_sale_assets', 'held_to_maturity_investment', 'longterm_account_receivable', 'fixed_assets', 'intangible_assets', 'research_and_development', 'goodwill', 'long_deferred_expense', 'deferred_tax_assets', 'noncurrent_assets', 'total_assets',
'shortterm_loan', 'trading_liability', 'note_payable', 'account_payable', 'salary_payable', 'dividend_payable', 'tax_payable', 'interest_payable', 'accrued_expense', 'deferred_proceed', 'current_liability',
'longterm_loan', 'bond_payable', 'longterm_account_payable', 'deferred_tax_liability', 'noncurrent_liability', 'liability',
'paidin_capital', 'capital_reserve_fund', 'surplus_reserve_fund', 'retained_earnings', 'se_without_mi', 'shareholder_equity', 'liability_and_equity',
'receivables', 'payables', 'invested_capital', 'noninterest_liability', 'interest_liability', 'tangible_equity', 'net_liability', 'working_capital', 'net_borrowing', 'net_operating_assets', 'net_operating_assets',
]
        delta_dnames = ['working_capital', 'fixed_assets', 'net_borrowing', 'net_operating_assets']
        cursor = self.db.jybs.find({'date': {'$lte': date}, 'sid': {'$in': list(sids)}}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        if len(df) == 0:
            return
        for dname in dnames:
            self.add_col(df, dname)
        self.add_col(df, 'note_receivable')
        self.add_col(df, 'dividend_receivable')
        self.add_col(df, 'interest_receivable')
        self.add_col(df, 'other_receivable')
        self.add_col(df, 'shortterm_bond_payable')
        self.add_col(df, 'other_payable')
        self.add_col(df, 'advance_receipt')
        self.add_col(df, 'other_noncurrent_liability')
        df['receivables'] = [self.norm(i1)+self.norm(i2)+self.norm(i3)+self.norm(i4)+self.norm(i5) for i1, i2, i3, i4, i5 in zip(df['note_receivable'], df['dividend_receivable'], df['interest_receivable'], df['account_receivable'], df['other_receivable'])]
        df['payables'] = [self.norm(i1)+self.norm(i2)+self.norm(i3)+self.norm(i4)+self.norm(i5)+self.norm(i6)+self.norm(i7)+self.norm(i8) for i1, i2, i3, i4, i5, i6, i7, i8 in zip(df['note_payable'], df['account_payable'], df['shortterm_bond_payable'], df['salary_payable'], df['dividend_payable'], df['tax_payable'], df['interest_payable'], df['other_payable'])]
        df['noninterest_ncl'] = [i-self.norm(j)-self.norm(k) for i, j, k in zip(df['noncurrent_liability'], df['longterm_loan'], df['bond_payable'])]
        df['noninterest_cl'] = [i1+self.norm(i2)+self.norm(i3)+self.norm(i4)+self.norm(i5)+self.norm(i6)+self.norm(i7)+self.norm(i8) for i1, i2, i3, i4, i5, i6, i7, i8 in zip(df['account_payable'], df['advance_receipt'], df['salary_payable'], df['tax_payable'], df['other_payable'], df['accrued_expense'], df['deferred_proceed'], df['other_noncurrent_liability'])]
        df['noninterest_liability'] = df['noninterest_ncl'] + df['noninterest_cl']
        df['interest_liability'] = df['liability'] - df['noninterest_liability']
        df['invested_capital'] = df['se_without_mi'] + df['interest_liability']
        df['tangible_equity'] = [i1-(self.norm(i2)+self.norm(i3)+self.norm(i4)+self.norm(i5)+self.norm(6)) for i1, i2, i3, i4, i5, i6 in zip(df['se_without_mi'], df['intangible_assets'], df['research_and_development'], df['goodwill'], df['long_deferred_expense'], df['deferred_tax_assets'])]
        df['net_liability'] = df['interest_liability'] - df['cash_and_equivalent']
        df['working_capital'] = df['current_assets'] - df['current_liability']
        df['net_borrowing'] = [i1+self.norm(i2)+i3+self.norm(i4)+self.norm(i5) for i1, i2, i3, i4, i5 in zip(df['shortterm_loan'], df['note_payable'], df['longterm_loan'], df['bond_payable'], df['shortterm_bond_payable'])]
        df['net_operating_assets'] = df['assets']-df['liability']+df['net_liability']
        dnames = [dname for dname in dnames if dname in df.columns]
        delta_dnames = [dname for dname in delta_dnames if dname in dnames]
        df['qtrno'] = 4*(df.year-2000) + df.quarter
        for sid, sdf in df.groupby('sid'):
            df = sdf[dnames+['qtrno', 'date', 'year']].sort(['qtrno', 'date']).drop_duplicates('qtrno', take_last=True)
            df.index = df.qtrno
            # dtype: 'Y'
            df1 = df.ix[df.qtrno % 4 == 0].copy()
            if len(df1):
                df1.year -= df1.year.iloc[-1]
                for i, y in df1.year.iteritems():
                    if y <= -2:
                        continue
                    key = {'date': date, 'sid': sid, 'dtype': 'Y'+str(y)}
                    doc = {dname: df1[dname][i] for dname in dnames}
                    doc.update({'qtrno': df1.qtrno.iloc[-1]})
                    doc.update({'delta_'+dname: np.nan for dname in delta_dnames})
                    pi = i-4
                    if pi in df1.index:
                        doc.update({'delta_'+dname: doc[dname] - df1[dname][pi] for dname in delta_dnames})
                    self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'S'
            df2 = df.ix[df.qtrno % 2 == 0].copy()
            if len(df2):
                cs = df2.qtrno.iloc[-1]
                for i in range(len(df2)-1, -1, -1):
                    s = df2.qtrno.iloc[i]
                    if (s-cs)/2 <= -4:
                        continue
                    key = {'date': date, 'sid': sid, 'dtype': 'S'+str((s-cs)/2)}
                    doc = {dname: df2[dname][s] for dname in dnames}
                    doc.update({'qtrno': df2.qtrno.iloc[-1]})
                    doc.update({'delta_'+dname: np.nan for dname in delta_dnames})
                    ps = s - 2
                    if ps in df1.index:
                        doc.update({'delta_'+dname: doc[dname] - df1[dname][ps] for dname in delta_dnames})
                    self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'Q'
            df3 = df.copy()
            if len(df3):
                cq = df3.qtrno.iloc[-1]
                for i in range(len(df3)-1, -1, -1):
                    q = df3.qtrno.iloc[i]
                    if q-cq <= -8:
                        continue
                    key = {'date': date, 'sid': sid, 'dtype': 'Q'+str(q-cq)}
                    doc = {dname: df3[dname][q] for dname in dnames}
                    doc.update({'qtrno': df3.qtrno.iloc[-1]})
                    doc.update({'delta_'+dname: np.nan for dname in delta_dnames})
                    pq = q - 1
                    if pq in df1.index:
                        doc.update({'delta_'+dname: doc[dname] - df1[dname][pq] for dname in delta_dnames})
                    self.collection.update(key, {'$set': doc}, upsert=True)
            # dtype: 'TTM'
            df4 = df.copy()
            if len(df4):
                cq = df4.qtrno.iloc[-1]
                if cq % 4 == 0:
                    key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                    doc = {dname: df4[dname][cq] for dname in dnames}
                    doc.update({'qtrno': df4.qtrno.iloc[-1]})
                    doc.update({'delta_'+dname: np.nan for dname in delta_dnames})
                    pq = cq - 4
                    if pq in df4.index:
                        doc.update({'delta_'+dname: doc[dname]-df4[dname][pq] for dname in delta_dnames})
                    self.collection.update(key, {'$set': doc}, upsert=True)
                else:
                    pyq, py = cq - 4, cq - cq % 4
                    ppyq, ppy = pyq - 4, py - 4
                    if py in df4.qtrno:
                        if pyq not in df4.qtrno:
                            key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                            doc = {dname: df4[dname][py] for dname in dnames}
                            doc.update({'qtrno': df4.qtrno.iloc[-1]})
                            doc.update({'delta_'+dname: np.nan for dname in delta_dnames})
                            if ppy in df4.index:
                                doc.update({'delta_'+dname: doc[dname]-df4[dname][ppy]})
                            self.collection.update(key, {'$set': doc}, upsert=True)
                        else:
                            key = {'date': date, 'sid': sid, 'dtype': 'TTM'}
                            doc = {dname: self.norm(df4[dname][py])-self.norm(df4[dname][pyq])+df4[dname][cq] for dname in dnames}
                            doc.update({'qtrno': df4.qtrno.iloc[-1]})
                            doc.update({'delta_'+dname: np.nan for dname in delta_dnames})
                            if ppy in df4.index and ppyq in df4.index:
                                doc.update({'delta_'+dname: doc[dname]-(self.norm(df4[dname][ppy])-self.norm(df4[dname][ppyq])+df4[dname][pyq]) for dname in dnames})
                            self.collection.update(key, {'$set': doc}, upsert=True)

if __name__ == '__main__':
    jydata = JYDataUpdater()
    jydata.run()
