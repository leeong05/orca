Index data
==========

* Fetcher: :py:class:`orca.mongo.index.IndexQuoteFetcher`

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
======================= =================================================

Example::

   index.fetch('close', '20140101', '20140301', index='HS300')


* Fetcher: :py:class:`orca.mongo.sywgquote.SYWGQuote.Fetcher`

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
======================= =================================================

Example::

   sywgquote.fetch('close', '20140101', '20140301', level=1, use_industry=True)
