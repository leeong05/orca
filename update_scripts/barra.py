"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
import json

import pandas as pd

from base import UpdaterBase
import barra_sql as sql


class BarraUpdater(UpdaterBase):
    """The updater class for collections ``biarra_D/S_exposure', 'barra_D/S_returns', 'barra_D/S_covariance', 'barra_D/S_specifics'.

    :param str model: Model version, currently only supports: ('daily', 'short')
    """

    def __init__(self, model='daily', offset=1, timeout=600, iterates=3, update_idmaps=True):
        self.model = model
        self.update_idmaps = update_idmaps
        super(BarraUpdater, self).__init__(offset=offset, timeout=timeout, iterates=iterates)

    def pre_update(self):
        self.dates = self.db.dates.distinct('date')
        if self.model == 'daily':
            self.__dict__.update({
                    'barra_idmaps': self.db.barra_idmaps,
                    'exposure': self.db.barra_D_exposure,
                    'facret': self.db.barra_D_returns,
                    'faccov': self.db.barra_D_covariance,
                    'precov': self.db.barra_D_precovariance,
                    'specifics': self.db.barra_D_specifics,
                    })
        else:
            self.__dict__.update({
                    'barra_idmaps': self.db.barra_idmaps,
                    'exposure': self.db.barra_S_exposure,
                    'facret': self.db.barra_S_returns,
                    'faccov': self.db.barra_S_covariance,
                    'precov': self.db.barra_S_precovariance,
                    'specifics': self.db.barra_S_specifics,
                    })
        if not self.skip_monitor:
            self.connect_monitor()

    def pro_update(self):
        pass

    def update(self, date):
        """Update factor exposure, factor returns, factor covariance and stocks specifics for **previous** day before market open."""
        if not os.path.exists(sql.gp_idmaps(date, self.model)):
            self.logger.error('Barra model data does not exist on {}', date)
            sql.fetch_and_parse(date)
        self.idmaps = json.load(open(sql.gp_idmaps(date, self.model)))
        if self.update_idmaps:
            self.barra_idmaps.update({'date': date}, {'$set': {'idmaps': self.idmaps}}, upsert=True)
        self.update_exposure(date)
        self.update_facret(date)
        self.update_faccov(date)
        self.update_precov(date)
        self.update_specifics(date)

    def monitor(self, date):
        statistics = ('count', 'mean', 'min', 'max', 'median', 'std', 'quartile1', 'quartile3')
        SQL1 = "SELECT * FROM mongo_barra WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL2 = "UPDATE mongo_barra SET value=%s WHERE trading_day=%s AND data=%s AND statistic=%s"
        SQL3 = "INSERT INTO mongo_barra (trading_day, data, statistic, value) VALUES (%s, %s, %s, %s)"

        cursor = self.monitor_connection.cursor()

        idmaps = self.barra_idmaps.find_one({'date': date})['idmaps']
        cursor.execute(SQL1, (date, 'idmaps', 'count'))
        if list(cursor):
            cursor.execute(SQL2, (len(idmaps), date, 'idmaps', 'count'))
        else:
            cursor.execute(SQL3, (date, 'idmaps', 'count', len(idmaps)))
        self.logger.info('MONITOR for {} on {}', 'idmaps', date)

        for collection in [self.exposure, self.specifics]:
            for dname in collection.distinct('dname'):
                ser = pd.Series(collection.find_one({'dname': dname, 'date': date})['dvalue'])
                for statistic in statistics:
                    cursor.execute(SQL1, (date, dname, statistic))
                    if list(cursor):
                        cursor.execute(SQL2, (self.compute_statistic(ser, statistic), date, dname, statistic))
                    else:
                        cursor.execute(SQL3, (date, dname, statistic, self.compute_statistic(ser, statistic)))
                self.logger.info('MONITOR for {} on {}', dname, date)
        self.monitor_connection.commit()

    def update_exposure(self, date):
        expjson = sql.gp_expjson(date, self.model)
        if not os.path.exists(expjson):
            self.logger.error('No record found for {} on {}', self.exposure.name, date)
            return
        expjson = json.load(open(expjson))

        for dname, dvalue in expjson.iteritems():
            key = {'date': date, 'dname': dname}
            self.exposure.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} factors into (c: [{}]) of (d: [{}]) on {}', len(expjson), self.exposure.name, self.db.name, date)

    def update_facret(self, date):
        facret = sql.gp_facret(date, self.model)
        if not os.path.exists(facret):
            self.logger.error('No record found for {} on {}', self.facret.name, date)
            return

        cnt = 0
        with open(facret) as file:
            for line in file:
                try:
                    factor, returns, _date = [item.strip() for item in line.split('|')]
                    assert date == _date
                    returns = float(returns) * 0.01
                    self.facret.update({'date': date, 'factor': factor}, {'$set': {'returns': returns}}, upsert=True)
                    cnt += 1
                except:
                    pass
        self.logger.info('UPSERT documents for {} factors into (c: [{}]) of (d: [{}]) on {}', cnt, self.facret.name, self.db.name, date)

    def update_faccov(self, date):
        faccov = sql.gp_faccov(date, self.model)
        if not os.path.exists(faccov):
            self.logger.error('No record found for {} on {}', self.faccov.name, date)
            return

        from collections import defaultdict
        res = defaultdict(dict)
        with open(faccov) as file:
            for line in file:
                try:
                    factor1, factor2, cov, _ = [item.strip() for item in line.split('|')]
                    cov = float(cov)
                    res[factor1][factor2] = cov
                    res[factor2][factor1] = cov
                except:
                    pass

        for factor, cov in res.iteritems():
            key = {'date': date, 'factor': factor}
            self.faccov.update(key, {'$set': {'covariance': cov}}, upsert=True)
        self.logger.info('UPSERT documents for {} factors into (c: [{}]) of (d: [{}]) on {}', len(res), self.faccov.name, self.db.name, date)

    def update_precov(self, date):
        precov = sql.gp_precov(date, self.model)
        if not os.path.exists(precov):
            self.logger.error('No record found for {} on {}', self.precov.name, date)
            return

        from collections import defaultdict
        res = defaultdict(dict)
        with open(precov) as file:
            for line in file:
                try:
                    factor1, factor2, cov, _ = [item.strip() for item in line.split('|')]
                    cov = float(cov)
                    res[factor1][factor2] = cov
                    res[factor2][factor1] = cov
                except:
                    pass

        for factor, cov in res.iteritems():
            key = {'date': date, 'factor': factor}
            self.precov.update(key, {'$set': {'covariance': cov}}, upsert=True)
        self.logger.info('UPSERT documents for {} factors into (c: [{}]) of (d: [{}]) on {}', len(res), self.precov.name, self.db.name, date)

    def update_specifics(self, date):
        specret = sql.gp_specret(date, self.model)
        specs = sql.gp_specs(date, self.model)
        if not os.path.exists(specs) or not os.path.exists(specret):
            self.logger.error('No record found for {} on {}', self.specifics.name, date)
            return

        specific_returns = {}
        with open(specret) as file:
            for line in file:
                try:
                    bid, sret, _ = [item.strip() for item in line.split('|')]
                    sret = float(sret)
                    specific_returns[self.idmaps[bid]] = sret
                except:
                    pass
        self.specifics.update({'dname': 'specific_returns', 'date': date}, {'$set': {'dvalue': specific_returns}}, upsert=True)

        dividend_yield, total_risk, specific_risk, historical_beta, predicted_beta = {}, {}, {}, {}, {}
        with open(specs) as file:
            for line in file:
                try:
                    bid, div, trisk, srisk, hbeta, pbeta, _ = [item.strip() for item in line.split('|')]
                    sid = self.idmaps[bid]
                except:
                    continue
                try:
                    dividend_yield[sid] = float(div)
                except:
                    pass
                try:
                    total_risk[sid] = float(trisk)
                except:
                    pass
                try:
                    specific_risk[sid] = float(srisk)
                except:
                    pass
                try:
                    historical_beta[sid] = float(hbeta)
                except:
                    pass
                try:
                    predicted_beta[sid] = float(pbeta)
                except:
                    pass
        self.specifics.update({'dname': 'dividend_yield', 'date': date}, {'$set': {'dvalue': dividend_yield}}, upsert=True)
        self.specifics.update({'dname': 'total_risk', 'date': date}, {'$set': {'dvalue': total_risk}}, upsert=True)
        self.specifics.update({'dname': 'specific_risk', 'date': date}, {'$set': {'dvalue': specific_risk}}, upsert=True)
        self.specifics.update({'dname': 'historical_beta', 'date': date}, {'$set': {'dvalue': historical_beta}}, upsert=True)
        self.specifics.update({'dname': 'predicted_beta', 'date': date}, {'$set': {'dvalue': predicted_beta}}, upsert=True)
        self.logger.info('UPSERT documents for {} sids into (c: [{}]) of (d: [{}]) on {}', len(specific_returns), self.specifics.name, self.db.name, date)


if __name__ == '__main__':
    #barra_daily = BarraUpdater(model='daily')
    #barra_daily.run()
    barra_short = BarraUpdater(model='short', update_idmaps=True)
    barra_short.run()
