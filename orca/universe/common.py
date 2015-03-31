"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca import (
        DAYS_IN_YEAR,
        DAYS_IN_QUARTER,
        )

from ops import (
        ChainFilter,
        NegateFilter,
        UnionFilter,
        )
from factory import (
        create_quote_filter,
        create_cap_filter,
        )
from special import (
        TickerFilter,
        TradingDaysFilter,
        ActiveFilter,
        ComponentsFilter,
        IndustryFilter,
        )
import rules

"""ticker filters"""

SH = TickerFilter(rule=rules.startswith('60'))
SZ = TickerFilter(rule=rules.startswith(['00', '30']))
CYB = TickerFilter(rule=rules.startswith('30'))
ZXB = TickerFilter(rule=rules.startswith('002'))
SZS = UnionFilter([CYB, ZXB])
SZB = ChainFilter([(SZ, NegateFilter(SZS))])

"""ipo filters"""
T1Y = TradingDaysFilter(int(1.25*DAYS_IN_YEAR), rules.count_pct_gt(80))

"""active filter: used in every backtesting alpha"""
ACTIVE = ActiveFilter()

"""index components filter"""
HS300 = ComponentsFilter('HS300')
CS500 = ComponentsFilter('CS500')
CS800 = ComponentsFilter('CS800')

"""industry filter"""
FINANCE = IndustryFilter(['480000', '490000'])
NONFIN = NegateFilter(FINANCE)
BANK = IndustryFilter(['480000'])
NONBANK = NegateFilter(BANK)

"""returns filter"""
ReturnsFilter = create_quote_filter('ReturnsFilter', 'returns')
ZDT = ReturnsFilter(1, rules.avg_abs_lte(0.098))

"""cap filters"""
TotalCapFilter = create_cap_filter('TotalCapFilter', 'total_shares')
TotalCap70Q = TotalCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(70))
TotalCap60Q = TotalCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(60))

TotalCapT30Q = TotalCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(30))
TotalCapB30Q = TotalCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(30, ascending=True))
TotalCapM40Q = NegateFilter(UnionFilter([TotalCapT30Q, TotalCapB30Q]))

FloatCapFilter = create_cap_filter('FloatCapFilter', 'float_shares')
FloatCap70Q = FloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(70))
FloatCap60Q = FloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(60))

FloatCapT30Q = FloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(30))
FloatCapB30Q = FloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(30, ascending=True))
FloatCapM40Q = NegateFilter(UnionFilter([FloatCapT30Q, FloatCapB30Q]))

FreeFloatCapFilter = create_cap_filter('FreeFloatCapFilter', 'free_float_shares')
FreeFloatCap70Q = FreeFloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(70))
FreeFloatCap60Q = FreeFloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(60))

FreeFloatCapT30Q = FreeFloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(30))
FreeFloatCapB30Q = FreeFloatCapFilter(DAYS_IN_QUARTER, rules.avg_rank_pct_lte(30, ascending=True))
FreeFloatCapM40Q = NegateFilter(UnionFilter([FreeFloatCapT30Q, FreeFloatCapB30Q]))

"""liquidity filters"""
AmountFilter = create_quote_filter('AmountFilter', 'amount')
Liq70 = AmountFilter(DAYS_IN_QUARTER, rules.sum_rank_pct_lte(70))
Liq60 = AmountFilter(DAYS_IN_QUARTER, rules.sum_rank_pct_lte(60))

"""composite filters factory methods"""

def create_topliquid_filter(cap, liq, window=DAYS_IN_QUARTER):
    return ChainFilter([T1Y, (TotalCapFilter(window, rules.sum_rank_pct_lte(cap)),
                              AmountFilter(window, rules.sum_rank_pct_lte(liq)))
                       ])

def create_backtesting_topliquid_filter(cap, liq, window=DAYS_IN_QUARTER):
    return ChainFilter([(create_topliquid_filter(cap, liq, window=window), ACTIVE)])
