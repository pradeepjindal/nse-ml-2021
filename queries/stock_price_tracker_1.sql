select a.symbol, round(avg_close,2) average, round(tdy_low/ (avg_close/100),1) avg_pct, tdy_low,
round(tdy_low/ (high/100),0) high_pct, high
from
(
    select symbol, max(low) high, avg(close) avg_close from nse_cash_market_tab
    where trade_date > to_Date('2021-04-01','yyyy-MM-dd')
    group by symbol
) a,
(
    select symbol, low tdy_low from nse_cash_market_tab
    where trade_date = (select max(trade_Date) dt from nse_cash_market_tab)
) b
where a.symbol = b.symbol
order by high_pct
