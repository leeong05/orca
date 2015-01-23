"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import os
import json

from base import UpdaterBase
import barra_sql


class BarraUpdater(UpdaterBase):
    """The updater class for collections ``biarra_D/S_exposure', 'barra_D/S_returns', 'barra_D/S_covariance', 'barra_D/S_specifics'.

    :param str model: Model version, currently only supports: ('daily', 'short')
    """

    def __init__(self, model='daily', timeout=600, iterates=3, update_idmaps=True):
        UpdaterBase.__init__(self, timeout, iterates)
        self.model = model
        self.update_idmaps = update_idmaps

    def pre_update(self):
        self.__dict__.update({'dates': self.db.dates.distinct('date')})
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
                    'precov': self.db.barra_D_precovariance,
                    'specifics': self.db.barra_S_specifics,
                    })

    def pro_update(self):
        return

        self.logger.debug('Ensuring index date_1 on collection {}', self.barra_idmaps.name)
        self.barra_idmaps.ensure_index([('date', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index dname_1_date_1 on collection {}', self.exposure.name)
        self.exposure.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index factor_1_date_1 on collection {}', self.facret.name)
        self.facret.ensure_index([('factor', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index date_1_factor_1 on collection {}', self.faccov.name)
        self.faccov.ensure_index([('date', 1), ('factor', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index date_1_factor_1 on collection {}', self.precov.name)
        self.precov.ensure_index([('date', 1), ('factor', 1)],
                unique=True, dropDups=True, background=True)
        self.logger.debug('Ensuring index dname_1_date_1 on collection {}', self.specifics.name)
        self.specifics.ensure_index([('dname', 1), ('date', 1)],
                unique=True, dropDups=True, background=True)

    def update(self, date):
        """Update factor exposure, factor returns, factor covariance and stocks specifics for **previous** day before market open."""
        date = self.dates[self.dates.index(date)-1]
        if not os.path.exists(barra_sql.gp_idmaps(date, self.model)):
            self.logger.error('Barra model data does not exist on {}', date)
            barra_sql.fetch_and_parse(date)
        self.idmaps = json.load(open(barra_sql.gp_idmaps(date, self.model)))
        if self.update_idmaps:
            self.barra_idmaps.update({'date': date}, {'date': date, 'idmaps': self.idmaps}, upsert=True)
        self.update_exposure(date)
        self.update_facret(date)
        self.update_faccov(date)
        self.update_precov(date)
        self.update_specifics(date)

    def update_exposure(self, date):
        expjson = barra_sql.gp_expjson(date, self.model)
        if not os.path.exists(expjson):
            self.logger.error('No record found for {} on {}', self.exposure.name, date)
            return
        expjson = json.load(open(expjson))

        for dname, dvalue in expjson.iteritems():
            key = {'date': date, 'dname': dname}
            self.exposure.update(key, {'$set': {'dvalue': dvalue}}, upsert=True)
        self.logger.info('UPSERT documents for {} factors into (c: [{}]) of (d: [{}]) on {}', len(expjson), self.exposure.name, self.db.name, date)

    def update_facret(self, date):
        facret = barra_sql.gp_facret(date, self.model)
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
        faccov = barra_sql.gp_faccov(date, self.model)
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
        precov = barra_sql.gp_precov(date, self.model)
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
        specret = barra_sql.gp_specret(date, self.model)
        specs = barra_sql.gp_specs(date, self.model)
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
