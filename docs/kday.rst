Standard KDay data
==================

* Fetcher: :py:class:`~orca.mongo.quote.QuoteFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
open
high
low
close
prev_close
volume               
amount
vwap                    ``amount / volume``
returns                 ``close / prev_close - 1.``
returnsN                see :ref:`Example<quote_example>`
======================= =================================================

.. _quote_example: 

Example::

   quote.fetch('returnsN', 20, '20140201', '20140301')


* Fetcher: :py:class:`~orca.mongo.adjquote.AdjQuoteFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
adj_open
adj_high
adj_low
adj_close
adj_prev_close
adj_volume               
adj_amount              ``amount``
adj_vwap                Not equal to ``adj_amount / adj_volume``
                        see :ref:`Example<adjquote_example>`
adj_returns             ``returns``
======================= =================================================

.. _adjquote_example:

Example::

   adjquote.fetch('adj_vwap', '20140201', '20140301', mode=adjquote.BACKWARD)


* Fetcher: :py:class:`~orca.mongo.kday.CaxFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
adjfactor               adjust price data
volfactor               adjust volume data
======================= =================================================


* Fetcher: :py:class:`~orca.mongo.kday.SharesFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
a_shares                A股
a_float                 流通A股
a_float_restricted      流通A股中有限售条件的A股
a_float_nonrestricted   流通A股中无限售条件的A股
======================= =================================================


* Fetcher: :py:class:`~orca.mongo.industry.IndustryFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
sector, level1
industry, level2
subindustry, level3
index                   see :ref:`Example<industry_example>`
name                    see :ref:`Example<industry_example>`
======================= =================================================

.. _industry_example:

Example::

   industry.fetch_info('name', level=1, standard='ZX')
   industry.fetch_info('index', level=1, standard='SW2014')


* Fetcher: :py:class:`~orca.mongo.components.ComponentsFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
*index code*            for example: 'SH000300';
                        see :ref:`Example<components_example>`
*index name*            for example: 'HS300'; 
                        see :ref:`Example<components_example>`
======================= =================================================

.. _components_example:

Example::

   components.fetch('HS300', '20140101', '20140301', as_bool=True)
   components.fetch('SH000300', '20140101', '20140301', as_bool=False)
