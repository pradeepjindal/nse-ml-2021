select *, b.PRICE - a.PRICE as diff
from
(
select trade_Date, expiry_date, ROUND((OPEN+HIGH+LOW+close)/4,2) PRICE from nse_future_market_tab
where symbol = 'UPL'
and expiry_date = to_date('28-01-2021','dd-mm-yyyy')
) a,
(
select trade_Date, expiry_date, ROUND((OPEN+HIGH+LOW+close)/4,2) PRICE from nse_future_market_tab
where symbol = 'UPL'
and expiry_date = to_date('25-02-2021','dd-mm-yyyy')
) b
where a.trade_date = b.trade_date and a.trade_Date > to_date('31-12-2020','dd-mm-yyyy')
order by a.trade_date
