CMD0 = """
SELECT
  trade_days
FROM
  AShareCalendar
WHERE
  trade_days = '{date}'
"""

CMD1_1 = """
SELECT
  LEFT(s_info_windcode, 6), change_dt, change_dt1, ann_dt,
  tot_shr, float_a_shr, restricted_a_shr, non_tradable_shr
FROM
  AShareCapitalization
"""

CMD1_2 = """
SELECT
  LEFT(s_info_windcode, 6), change_dt, change_dt1, ann_dt, s_share_freeshares
FROM
  AShareFreeFloat
"""
