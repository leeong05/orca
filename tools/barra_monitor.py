"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import mysql.connector

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import DateFormatter

import logbook
logger = logbook.Logger('risk')

from orca.mongo.barra import (
        BarraFactorFetcher,
        BarraExposureFetcher,
        BarraSpecificsFetcher,
        )

factor_fetcher = BarraFactorFetcher('short')
exposure_fetcher = BarraExposureFetcher('short')
specifics_fetcher = BarraSpecificsFetcher('short')

def vra_multiplier(date):
    cov = factor_fetcher.fetch_daily_covariance(date)
    pre = factor_fetcher.fetch_daily_covariance(date, prevra=True)
    mul = cov / pre
    try:
        assert len(np.unique((mul*100).astype(int))) == 1
    except:
        logger.error('Wrong data input for {}', date)
    return np.mean(mul.values)

def industry_style(date):
    industry_exposure = exposure_fetcher.fetch_daily('industry', date)
    style_exposure = exposure_fetcher.fetch_daily('style', date)
    res = {}
    for industry, stocks in industry_exposure.iteritems():
        stocks = stocks.dropna()
        style = style_exposure.ix[stocks.index].mean()
        res[industry] = style
    return pd.DataFrame(res).T

def specific_risk(startdate, enddate):
    srisk = specifics_fetcher.fetch('specific_risk', startdate, enddate).quantile(0.5, axis=1)
    trisk = specifics_fetcher.fetch('total_risk', startdate, enddate).quantile(0.5, axis=1)
    return srisk/trisk

def country_risk(startdate, enddate):
    cov = factor_fetcher.fetch_variance('CNE5S_COUNTRY', startdate, enddate)
    return np.sqrt(cov)

def connect_mysql():
    con = mysql.connector.connect(
            host='192.168.1.183',
            user='kbars',
            password='123456',
            database='monitor',
            )
    return con

def insert_barra_risk(con, multiplier, risk, specific, force=False):
    cur = con.cursor()
    cur.execute("""SELECT trading_day FROM barra_risk ORDER BY trading_day""")
    existing_dates = [item[0] for item in list(cur)]
    sql1 = (
        "INSERT INTO barra_risk (trading_day, multiplier, country_risk, specific_risk) "
        "VALUES (%s, %s, %s, %s)")
    sql2 = (
        "UPDATE barra_risk SET multiplier = %s, country_risk = %s, specific_risk = %s "
        "WHERE trading_day = %s")
    for date in multiplier.index:
        if date in existing_dates:
            if force:
                values = (float(multiplier[date]), float(risk[date]), float(specific[date]), date)
                cur.execute(sql2, values)
                logger.info('Update {} into barra_risk', values)
        else:
            values = (date, float(multiplier[date]), float(risk[date]), float(specific[date]))
            cur.execute(sql1, values)
            logger.info('Insert {} into barra_risk', values)
    con.commit()

def fetch_barra_risk(con, dates):
    cur = con.cursor()
    sql = (
        "SELECT trading_day, multiplier, country_risk, specific_risk "
        "FROM barra_risk "
        "WHERE trading_day >= '{}' AND trading_day <= '{}' "
        "ORDER BY trading_day").format(dates[0], dates[-1])
    cur.execute(sql)
    df = pd.DataFrame(list(cur))
    if len(df) > 0:
        df.columns = ['date', 'multiplier', 'risk', 'specific']
        df = df.drop_duplicates('date')
        df.index = df.date
    return df

def insert_barra_exposure(con, dates, force=True):
    cur = con.cursor()
    cur.execute("""SELECT DISTINCT trading_day FROM barra_exposure ORDER BY trading_day""")
    existing_dates = [item[0] for item in list(cur)]
    sql = (
        "INSERT INTO barra_exposure (trading_day, industry, style, exposure) "
        "VALUES (%s, %s, %s, %s)")
    for date in dates:
        if not force and date in existing_dates:
            continue
        df = industry_style(date)
        for ind, row in df.iterrows():
            for sty, exposure in row.iteritems():
                values = (date, ind[6:], sty[6:], float(exposure))
                cur.execute(sql, values)
        logger.info('Insert exposure data for date {}', date)
        con.commit()

def plot_risk(multiplier, risk, specific):
    multiplier.index, risk.index, specific.index = pd.to_datetime(multiplier.index), pd.to_datetime(risk.index), pd.to_datetime(specific.index)
    fig1, ax1_1 = plt.subplots()
    ax1_1.plot(multiplier.index, multiplier, 'r')
    ax1_1.format_xdata = DateFormatter('%Y%m%d')
    ax1_1.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    ax1_1.set_ylabel('multiplier', color='r')
    fig1.autofmt_xdate()

    ax1_2 = ax1_1.twinx()
    ax1_2.plot(risk.index, risk, 'b')
    ax1_2.set_ylabel('risk', color='b')

    fig2, ax2_1 = plt.subplots()
    ax2_1.plot(specific.index, specific, 'r')
    ax2_1.format_xdata = DateFormatter('%Y%m%d')
    ax2_1.xaxis.set_major_formatter(DateFormatter('%Y%m%d'))
    ax2_1.set_ylabel('specific', color='r')

    ax2_2 = ax2_1.twinx()
    ax2_2.plot(risk.index, risk, 'b')
    ax2_2.set_ylabel('risk', color='b')
    fig2.autofmt_xdate()

    pp = PdfPages('risk.pdf')
    pp.savefig(fig1)
    pp.savefig(fig2)
    pp.close()
    logger.info('Saved figure in {}', 'risk.pdf')


if __name__ == '__main__':
    import argparse
    from datetime import datetime
    from orca import DATES

    today = datetime.now().strftime('%Y%m%d')
    parser = argparse.ArgumentParser()
    parser.add_argument('date', default=today, nargs='?')
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', default=today, nargs='?')
    parser.add_argument('-f', '--force', action='store_true')
    parser.add_argument('-v', '--verify', action='store_true')
    parser.add_argument('-p', '--plot', action='store_true')
    parser.add_argument('-o', '--offset', type=int, default=1)
    args = parser.parse_args()

    if args.start and args.end:
        _dates = [date for date in DATES if date >= args.start and date <= args.end]
    else:
        _dates = [args.date] if args.date in DATES else []
    dates = []
    for date in _dates:
        di = DATES.index(date)-args.offset
        if di < 0:
            di = 0
        date = DATES[di]
        if date not in dates:
            dates.append(date)

    if not dates:
        logger.warning('No valid trading dates found with given options')
        exit(0)

    con = connect_mysql()
    if not args.force:
        df = fetch_barra_risk(con, dates)
        if len(df) == len(dates):
            multiplier, risk, specific = df.multiplier, df.risk, df.specific
        else:
            risk = country_risk(dates[0], dates[-1])
            multiplier = pd.Series()
            for date in dates:
                if (len(df) and date not in df.index) or not len(df):
                    multiplier.ix[date] = vra_multiplier(date)
            specific = specific_risk(dates[0], dates[-1])
            if args.verify:
                ans = raw_input('Insert new data into barra_risk?[y/n]')
                if ans == 'y':
                    insert_barra_risk(con, multiplier, risk, specific, args.force)
            else:
                insert_barra_risk(con, multiplier, risk, specific, args.force)
    else:
        risk = country_risk(dates[0], dates[-1])
        multiplier = pd.Series()
        for date in dates:
            multiplier.ix[date] = vra_multiplier(date)
        specific = specific_risk(dates[0], dates[-1])

        if args.verify:
            ans = raw_input('Insert new data into barra_risk?[y/n]')
            if ans == 'y':
                insert_barra_risk(con, multiplier, risk, specific, args.force)
        else:
            insert_barra_risk(con, multiplier, risk, specific, args.force)

    insert_barra_exposure(con, dates, args.force)
    if args.plot:
        plot_risk(multiplier, risk, specific)
