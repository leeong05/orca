Minute-bar data
===============

* Fetcher: :py:class:`~orca.mongo.interval.IntervalFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
open                    区间开盘价
close                   区间收盘价
high                    
low
volume
amount
bid1                    区间最后时刻买一价，以此类推
bid2
bid3
bid4
bid5
ask1
ask2
ask3
ask4
ask5
bds1                    区间最后时刻买一手数，一次类推
bds2
bds3
bds4
dbs5
aks1
aks2
aks3
aks4
aks5
bvolume                 区间主动买入股数
bamount                 区间主动卖出股数
svolume                 区间主动买入量
samount                 区间主动卖出量
iwbds                   区间委托买入量
iwaks                   区间委托卖出量
======================= =================================================

Example::

   from orca.utils.dateutil import generate_timestamps as gen_ts
   times = gen_ts('130500', '150000', step=300, exclude_end=False)
   tsmin.fetch('close', times, '20140101', '20140301') 

* Fetcher: :py:class:`~orca.mongo.interval.IntervalReturnsFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
returns1                returns resampled every 1 minute
returns5                returns resampled every 5 minutes
returns15               returns resampled every 15 minutes
returns30               returns resampled every 30 minutes
returns60               returns resampled every 60 minutes
returns120              returns resampled every 120 minutes
======================= =================================================
