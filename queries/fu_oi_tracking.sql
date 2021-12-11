select t1.symbol, trade_Date, close, avg_oi, expiry_month, oi_growth
from
(
	select symbol, trade_Date, close from nse_cash_market_tab
	where trade_Date = (select max(trade_Date) from nse_cash_market_tab)
) t1,
(
	select aa.symbol, min_expiry_date, all_avg_oi, avg_oi, expiry_month, round((avg_oi/all_avg_oi)*100) oi_growth
	from
	(
		SELECT abc.symbol, abc.min_expiry_date, to_char(min_expiry_date,'yyyy-mm') expiry_month, round(avg(open_int)) avg_oi
		FROM (
				SELECT ft.symbol, ft.trade_date, min(ft.expiry_date) AS min_expiry_date
				FROM nse_future_market_tab ft
				--where symbol = 'COLPAL'
				GROUP BY ft.symbol, ft.trade_date
			) abc,
			(
				select symbol, trade_Date, expiry_Date, open_int from nse_future_market_tab
			) xyz
		where abc.symbol = xyz.symbol and abc.trade_date = xyz.trade_date and abc.min_expiry_Date = xyz.expiry_Date
		--and abc.symbol = 'COLPAL'
		group by abc.symbol, abc.min_expiry_date
	) aa,
	(
		SELECT abc.symbol, round(avg(open_int)) all_avg_oi
		FROM (
				SELECT ft.symbol, ft.trade_date, min(ft.expiry_date) AS min_expiry_date
				FROM nse_future_market_tab ft
				--where symbol = 'COLPAL'
				GROUP BY ft.symbol, ft.trade_date
			) abc,
			(
				select symbol, trade_Date, expiry_Date, open_int from nse_future_market_tab
			) xyz
		where abc.symbol = xyz.symbol and abc.trade_date = xyz.trade_date and abc.min_expiry_Date = xyz.expiry_Date
		--and abc.symbol = 'COLPAL'
		group by abc.symbol
	) bb
	where aa.symbol = bb.symbol
	and expiry_month = '2021-12'
	order by oi_growth
) t2
where t1.symbol = t2.symbol
order by oi_growth
