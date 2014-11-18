import os
import json
import logging

logger = logging.getLogger('updater')

from base import UpdaterBase
import barra_sql

"""
The updater class for collections 'barra_D/S_exposure', 'barra_D/S_returns', 'barra_D/S_covariance', 'barra_D/S_specifics'
"""

class BarraUpdater(UpdaterBase):

    def __init__(self, model='daily', timeout=300):
        UpdaterBase.__init__(self, timeout)
        self.model = model

    def pre_update(self):
        self.__dict__.update({'dates': self.db.dates.distinct('date')})
        if self.model == 'daily':
            self.__dict__.update({
                    'idmaps': None,
                    'exposure': self.db.barra_D_exposure,
                    'facret': self.db.barra_D_returns,
                    'faccov': self.db.barra_D_covariance,
                    'specifics': self.db.barra_D_specifics,
                    })
        else:
            self.__dict__.update({
                    'idmaps': None,
                    'exposure': self.db.barra_S_exposure,
                    'facret': self.db.barra_S_returns,
                    'faccov': self.db.barra_S_covariance,
                    'specifics': self.db.barra_S_specifics,
                    })

    def pro_update(self):
        return

        logger.debug('Ensuring index date_1_dname_1 on collection %s', self.exposure.name)
        self.exposure.ensure_index([('date', 1), ('dname', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index dname_1_date_1 on collection %s', self.exposure.name)
        self.exposure.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index date_1_factor_1 on collection %s', self.facret.name)
        self.facret.ensure_index([('date', 1), ('factor', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index factor_1_date_1 on collection %s', self.facret.name)
        self.facret.ensure_index([('factor', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index date_1_factor_1 on collection %s', self.faccov.name)
        self.faccov.ensure_index([('date', 1), ('factor', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index date_1_dname_1 on collection %s', self.specifics.name)
        self.specifics.ensure_index([('date', 1), ('dname', 1)],
                unique=True, dropDups=True, background=True)
        logger.debug('Ensuring index dname_1_date_1 on collection %s', self.specifics.name)
        self.specifics.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        date = self.dates[self.dates.index(date)-1]
        if not os.path.exists(barra_sql.gp_idfile(date)):
            logger.error('Barra model data does not exist on %s', date)
            barra_sql.fetch_and_parse(date)
        self.idmaps = json.load(open(barra_sql.gp_idmaps(date)))
        self.update_exposure(date)
        self.update_facret(date)
        self.update_faccov(date)
        self.update_specifics(date)

    def update_exposure(self, date):
        expjson = barra_sql.gp_expjson(date, self.model)
        if not os.path.exists(expjson):
            logger.error('No record found for %s on %s', self.exposure.name, date)
            return
        expjson = json.load(open(expjson))

        for dname, dvalue in expjson.iteritems():
            key = {'date': date, 'dname': dname}
            self.exposure.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        logger.info('UPSERT documents for %d factors into (c: [%s]) of (d: [%s]) on %s', len(expjson), self.exposure.name, self.db.name, date)

    def update_facret(self, date):
        facret = barra_sql.gp_facret(date, self.model)
        if not os.path.exists(facret):
            logger.error('No record found for %s on %s', self.facret.name, date)
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
        logger.info('UPSERT documents for %d factors into (c: [%s]) of (d: [%s]) on %s', cnt, self.facret.name, self.db.name, date)

    def update_faccov(self, date):
        faccov = barra_sql.gp_faccov(date, self.model)
        if not os.path.exists(faccov):
            logger.error('No record found for %s on %s', self.faccov.name, date)
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
        logger.info('UPSERT documents for %d factors into (c: [%s]) of (d: [%s]) on %s', len(res), self.faccov.name, self.db.name, date)

    def update_specifics(self, date):
        specret = barra_sql.gp_specret(date, self.model)
        specs = barra_sql.gp_specs(date, self.model)
        if not os.path.exists(specs) or not os.path.exists(specret):
            logger.error('No record found for %s on %s', self.specifics.name, date)
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
        logger.info('UPSERT documents for %d sids into (c: [%s]) of (d: [%s]) on %s', len(specific_returns), self.specifics.name, self.db.name, date)


if __name__ == '__main__':
    barra_daily = BarraUpdater(model='daily')
    barra_daily.run()
    barra_short = BarraUpdater(model='short')
    barra_short.run()
