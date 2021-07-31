import datetime

import psycopg2

main_sql ="""
  SELECT
    tdy.symbol AS symbol,
    tdy.one_trade_date AS trade_date,

    tdy.cmclose_prev AS previous_close,
    tdy.cmopen AS open,
    tdy.cmhigh AS high,
    tdy.cmlow AS low,
    tdy.cmclose AS close,
    tdy.cmlast AS last,
    tdy.cmatp AS atp,

    round(tdy.cmopen / (yes.cmopen/100),2) - 100 AS open_chg_prcnt,
    round(tdy.cmhigh / (yes.cmhigh/100),2) - 100 AS high_chg_prcnt,
    round(tdy.cmlow / (yes.cmlow/100),2) - 100 AS low_chg_prcnt,
    round(tdy.cmclose / (yes.cmclose/100),2) - 100 AS close_chg_prcnt,
    round(tdy.cmlast / (yes.cmlast/100),2) - 100 AS last_chg_prcnt,
    round(tdy.cmatp / (yes.cmatp/100),2) - 100 AS atp_chg_prcnt,

    case when yes.traded_qty/100 = 0 then 0 else round(tdy.traded_qty / (yes.traded_qty/100),2) - 100 end AS volume_chg_prcnt,
    case when yes.delivered_qty/100 = 0 then 0 else round(tdy.delivered_qty / (yes.delivered_qty/100),2) - 100 end AS delivery_chg_prcnt,

    round(tdy.cmopen / (tdy.cmclose_prev/100), 2) - 100 AS close_to_open_percent,
    round(tdy.cmhigh / (tdy.cmopen/100), 2) - 100 AS othigh_prcnt,
    round(tdy.cmlow / (tdy.cmopen/100), 2) - 100 AS otlow_prcnt,
    round(tdy.cmclose / (tdy.cmopen/100), 2) - 100 AS otclose_prcnt,
    round(tdy.cmlast / (tdy.cmopen/100), 2) - 100 AS otlast_prcnt,
    round(tdy.cmatp / (tdy.cmopen/100), 2) - 100 AS otatp_prcnt,

    tdy.cmclose - yes.cmclose AS tdyclose_minus_yesclose,
    tdy.cmlast - yes.cmlast AS tdylast_minus_yeslast,
    tdy.cmatp - yes.cmatp AS tdyatp_minus_yesatp,
    tdy.delivered_qty - yes.delivered_qty AS tdydel_minus_yesdel,

    tdy.traded_qty volume,
    tdy.delivered_qty delivery,
    tdy.fucontracts,
    tdy.fu_tot_trd_val,
    tdy.fuOi
  FROM
    (
      select symbol, min(trade_Date) fut_traded_from_date, max(trade_date) fut_traded_to_date
      from nse_future_market_tab group by symbol
    ) distinct_future_stocks,
    (
      select *
      from cm_date_linking_view1 v1, cfd_data_cd_left_join_f_mv d1
      where v1.One_trade_date = d1.trade_Date
    ) tdy,
    (
      select *
      from cm_date_linking_view1 v2, cfd_data_cd_left_join_f_mv d2
      where v2.two_trade_date = d2.trade_Date
    ) yes
  WHERE tdy.symbol = distinct_future_stocks.symbol
  and tdy.trade_date >= distinct_future_stocks.fut_traded_from_date and tdy.trade_date <= distinct_future_stocks.fut_traded_to_date
  and tdy.one_trade_date = yes.one_trade_Date and tdy.symbol = yes.symbol
  and tdy.fuOi is not null
  order by tdy.symbol, tdy.trade_date
"""

connection = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port=5433)

cursor = connection.cursor()

# cursor.execute("SELECT * from nse_cash_market_tab where symbol='SBIN';")
cursor.execute(main_sql)

# cursor.execute("COPY " + main_sql + " TO * DELIMITER ',' CSV;")

one_record = cursor.fetchone()
print(one_record)

symbol = one_record[0]
high = one_record[2]

# print(datetime.datetime.now())
# records = cursor.fetchall()
# print(datetime.datetime.now())

# for each_record in records:
#     print(each_record)

# for symbol in cursor.fetchall():
#     print(symbol)

# print("Data from Database:- ", records)

connection.close()

