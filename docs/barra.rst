Barra model data
================

* Fetcher: :py:class:`~orca.mongo.barra.BarraFetcher`

Example::

   # special method
   barra2sid = barra.fetch_idmaps(date=None, barra_key=True)
   sid2barra = barra.fetch_idmaps(date=None, barra_key=False)


* Fetcher: :py:class:`~orca.mongo.barra.BarraSpecificsFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
specific_returns
dividend_yield
total_risk
specific_risk
historical_beta
predicted_beta
======================= =================================================


* Fetcher: :py:class:`~orca.mongo.barra.BarraExposureFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
*single factor name*    for example: 'CNE5D_COUNTRY'
*list of factor names*  for example: ['CNE5D_COUNTRY']
industry
style
(*not given*)           
======================= =================================================

Example::

   exposure.fetch_daily('CNE5D_COUNTRY', '20140104', offset=1)
   exposure.fetch_daily(['CNE5D_COUNTRY'], '20140104', offset=1)
   exposure.fetch_daily('industry', '20140104', offset=1)
   exposure.fetch_daily('20140104', offset=1)


* Fetcher: :py:class:`~orca.mongo.barra.BarraFactorFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
returns
covariance
*factor name*           dot **NOT** use in 
                        :py:meth:`~orca.mongo.barra.BarraFactorFetcher.fetch_daily`
======================= =================================================

Example::

   factor.fetch_window('CNE5D_COUNTRY', '20140101', '20140301')
   factor.fetch_window('returns', '20140101', '20140301')
   factor.fetch_daily('returns', '20140104', offset=1)
   factor.fetch_daily('covariance', '20140104', offset=1)


* Fetcher: :py:class:`~orca.mongo.barra.BarraCovarianceFetcher`

Example::

   covariance.fetch_daily('20140104', offset=1)
