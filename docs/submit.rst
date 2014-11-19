Submit Alpha
============

When an alpha is scrutinized by various performance and correlation criteria, it is ready to be submitted into the Alpha DB. The author is required to provide some descriptional information about this alpha; for example, its fitting universe, its category etc. Most importantly, **the source code must be re-constructed to facilitate daily update**.

Some guidelines to code re-construction:

* Replace :py:class:`orca.alpha.base.BacktestingAlpha` by :py:class:`orca.alpha.base.ProductionAlpha`
* Replace any data cache dependency by MongoDB APIs
* Remove any redundant or un-used data
* Check for forward-looking bias

Usually data items used in an alpha have considerable overlap with existing alphas of similar categories, Orca will provide a mechanism to group these alpha codes in order and schedule update flexible enought to reduce database pressure.

Example
-------

Research code. File ``cache.py``::

   from orca import DATES
   from orca.mongo import QuoteFetcher

   quote = QuoteFetcher()
   returns5 = quote.fetch('returns5', 20090101, DATES[-5])
  
   from orca.data import HDFSaver
   hdf = HDFSaver('./.cache')
   hdf['returns5'] = returns5

File ``alpha.py``::

   from orca.data import HDFLoader

   hdf = HDFLoader('./.cache')
   returns5 = hdf['returns5']

   from orca.alpha import BacktestingAlpha

   class AlphaReturns5(BacktestingAlpha):

       def __init__(self):
           super(AlphaReturns5, self).__init__()
           self.returns5 = returns5.shift(1)

       def generate(self, date):
           self.alphas[date] = -self.returns5.ix[date]

To submit, these two files should be merged and re-constructed as::

    from orca.mongo import QuoteFetcher
    from orca.alpha import ProdcutionAlpha

    class AlphaReturns5(ProdcutionAlpha):

        def __init__(self):
            super(AlphaReturns5, self).__ini__()
            self.quote = QuoteFetcher(datetime_index=True, reindex=True)

        def generate(self, date):
            returns5 = self.quote.fetch_daily('returns5', date, offset=1)
            return returns5
