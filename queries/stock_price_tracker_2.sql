select a.symbol, trade_Date, round(avg_close,2) average, round(tdy_low/ (avg_close/100),1) avg_pct, tdy_low,
round(tdy_low/ (high/100),0) high_pct, high
from
(
    select symbol, max(low) high, avg(close) avg_close from nse_cash_market_tab
    where trade_date > to_Date('2021-04-01','yyyy-MM-dd')
    group by symbol
) a,
(
    select symbol, trade_date, low tdy_low from nse_cash_market_tab
    where trade_date = (select max(trade_Date) dt from nse_cash_market_tab)
) b,
(
    select distinct symbol from nse_future_market_tab
    where trade_date > (select max(trade_Date)-10 dt from nse_cash_market_tab)
) c
where a.symbol = b.symbol and a.symbol = c.symbol
order by high_pct
