Introduction
============

Orca is a Python library specifically designed for backtesting **statistical arbitrage** strategies(*"alpha"* henceforth) on China A-Shares market.

This library is integrated with a delibrately structured **MongoDB** as its data source. It depends heavily on the high-performance Python library `Pandas <http://pandas.pydata.org/>`_, thus it is recommended to get familiar with its basic data structures(Series, DataFrame, Panel).

To experiment with Orca, it is advised that one creates a virtual Python environment with :program:`virtualenv`::

   $virtualenv /your/local/directory

and install the following dependency libraries properly:

* `Pandas <http://pandas.pydata.org/>`_
* `PyMongo <http://api.mongodb.org/python/current/>`_
* `PyTables <http://www.pytables.org/moin/>`_
* `TA-Lib <https://github.com/mrjbq7/ta-lib>`_

Orca has 8 main components:

* MongoDB interface
* Data cache
* Universes
* Alphas
* Performance
* Utilities
* Updaters
* Alpha DB interface

For backtesting purpose, one only have to focus on the first 6 components which are explained in detail in this documentation.
