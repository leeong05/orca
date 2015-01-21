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
vwap                    ``amount / volume``
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
   times = gen_ts('130500', '150000', step=300, exclude_end=False, exclude_begin=False)
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

* Fetcher: :py:class:`~orca.mongo.interval.AdjIntervalFetcher`

======================= =================================================
Data name               Remark
======================= =================================================
adj_open                
adj_close               
adj_high                    
adj_low
adj_vwap                Not equal to ``adj_amount / adj_volume``
adj_volume
adj_amount              ``amount`` 
adj_bid1               
adj_bid2
adj_bid3
adj_bid4
adj_bid5
adj_ask1
adj_ask2
adj_ask3
adj_ask4
adj_ask5
adj_bds1              
adj_bds2
adj_bds3
adj_bds4
adj_dbs5
adj_aks1
adj_aks2
adj_aks3
adj_aks4
adj_aks5
adj_bvolume          
adj_bamount             ``bamount``
adj_svolume        
adj_samount             ``samount``
adj_iwbds        
adj_iwaks
adj_returns             ``returns``
======================= =================================================

Example::

   from orca.mongo.interval import AdjIntervalFetcher
   adj = AdjIntervalFetcher('5min')
   adj.fetch_intervals('adj_returns', '20141209', '093500', 48, offset=1)
