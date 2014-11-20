universe -- Universe filters
============================

universe.base
-------------

.. automodule:: orca.universe.base
   :exclude-members: __weakref__, __module__, __dict__, __abstractmethods__

universe.ops
------------

.. automodule:: orca.universe.ops
   :exclude-members: __weakref__, __module__, __dict__, __abstractmethods__

universe.rules
--------------

.. automodule:: orca.universe.rules
   :exclude-members: __weakref__, __module__, __dict__, __abstractmethods__

universe.special
----------------

.. automodule:: orca.universe.special
   :exclude-members: __weakref__, __module__, __dict__, __abstractmethods__

universe.factory
----------------

.. automodule:: orca.universe.factory
   :exclude-members: __weakref__, __module__, __dict__, __abstractmethods__

universe.common
---------------

ticker filters
^^^^^^^^^^^^^^

.. py:data:: orca.universe.common.SH
   
   Filter that is true for stocks listed in Shanghai market.

.. py:data:: orca.universe.common.SZ

   Filter that is true for stocks listed in Shenzheng market.

.. py:data:: orca.universe.common.CYB

   Filter that is true for stocks listed in ChiNasdaq of Shenzheng market.

.. py:data:: orca.universe.common.ZXB

   Filter that is true for stocks listed in Mid&Small Entreprises Board of Shenzheng market.

.. py:data:: orca.universe.common.SZS

   Filter that is a union of :py:data:`~orca.universe.common.CYB` and :py:data:`~orca.universe.common.ZXB`.

.. py:data:: orca.universe.common.SZB

   Filter that excludes :py:data:`~orca.universe.common.SZS` from :py:data:`~orca.universe.common.SZ`.

IPO filters
^^^^^^^^^^^

.. py:data:: orca.universe.common.T1Y

   Filter that is true for stocks have at least 1 year's trading days in the last one year plus a quarter.

active filter
^^^^^^^^^^^^^

.. py:data:: orca.universe.common.ACTIVE

   Filter that is true on a day for stocks that are actively traded that day.

index components filter
^^^^^^^^^^^^^^^^^^^^^^^

.. py:data:: orca.universe.common.HS300
.. py:data:: orca.universe.common.CS500
.. py:data:: orca.universe.common.CS800

industry filter
^^^^^^^^^^^^^^^

.. py:data:: orca.universe.common.FINANCE

   Filter that is true for stocks in finance (banking and derivative banking industries) as defined by SW2014.
   
.. py:data:: orca.universe.common.NONFIN

   Filter that is the negate of :py:data:`~orca.universe.common.FINANCE`.

.. py:data:: orca.universe.common.BANK
.. py:data:: orca.universe.common.NONBANK

cap filter
^^^^^^^^^^

.. py:class:: orca.universe.common.TotalCapFilter

   Subclass of :py:class:`orca.universe.base.DataFilter` created by factory function :py:func:`orca.universe.factory.create_cap_filter` with ``shares=a_shares``.

.. py:class:: orca.universe.common.TotalCap70
.. py:class:: orca.universe.common.TotalCap60

   Filter that is true for stocks ranked in top 70/60 percent in terms of average total capitalization size within the last quarter.

.. py:class:: orca.universe.common.TotalCapT30
.. py:class:: orca.universe.common.TotalCapB30
.. py:class:: orca.universe.common.TotalCapM40

   These three filters cut the whole universe into top 30, bottom 30 and middle 40 (i.e. big, small, midium companies) of average total capitalization size within the last quarter.

.. py:class:: orca.universe.common.FloatCapFilter

   Subclass of :py:class:`orca.universe.base.DataFilter` created by factory function :py:func:`orca.universe.factory.create_cap_filter` with ``shares=a_float_nonrestricted``.

.. py:class:: orca.universe.common.FloatCap70
.. py:class:: orca.universe.common.FloatCap60

   Filter that is true for stocks ranked in top 70/60 percent in terms of average float capitalization size within the last quarter.

.. py:class:: orca.universe.common.FloatCapT30
.. py:class:: orca.universe.common.FloatCapB30
.. py:class:: orca.universe.common.FloatCapM40

   These three filters cut the whole universe into top 30, bottom 30 and middle 40 (i.e. big, small, midium companies) of average float capitalization size within the last quarter.

liquidity filters
^^^^^^^^^^^^^^^^^

.. py:class:: orca.universe.common.AmountFilter

   Sublcass of :py:class:`orca.universe.base.SimpleDataFilter` created by factory function :py:func:`orca.universe.factory.create_quote_filter` with ``dname=amount``.

.. py:class:: orca.universe.common.Liq70
.. py:class:: orca.universe.common.Liq60
   
   Filter that is true for stocks ranked in top 70/60 percent in terms of average trading amount within the last quarter.

factory for cap&liquidity composite filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. py:function:: orca.universe.common.create_topliquid_filter(cap, liq, window=61)

   :param numeric cap, liq: The percentage threshold in terms of total capitalization/trading amount

.. py:function:: orca.universe.common.create_backtesting_topliquid_filter(cap, liq, window=61)

   For backtesting, any filter created by :py:func:`~orca.universe.common.create_topliquid_filter` should be intersected with :py:data:`orca.universe.common.ACTIVE`.

