--select * from nse_option_market_tab
SELECT
	a.symbol, a.trade_date, b.expiry_date, round(pe_oi/lot_size,0) put_oi, round(ce_oi/lot_size,0) call_oi
	, CASE when ce_oi > 100.00 then round( (pe_oi/lot_size) / (ce_oi/lot_size), 2) else 0 END pcr
FROM
(
	select distinct symbol, trade_date, lot_size from nse_option_market_tab
	where tdn=20211122 order by symbol
) a,
(
	select symbol, trade_date, expiry_date, sum(open_int) ce_oi from nse_option_market_tab
	where option_type = 'CE' and tdn=20211122 and fedn=20211125
	group by symbol, trade_date, expiry_date
) b,
(
	select symbol, trade_date, expiry_date, sum(open_int) pe_oi from nse_option_market_tab
	where option_type = 'PE' and tdn=20211122 and fedn=20211125
	group by symbol, trade_date, expiry_date
) c
WHERE
	a.symbol = b.symbol and a.trade_date = b.trade_date
	and a.symbol = c.symbol and a.trade_date = c.trade_date
	and b.expiry_date = c.expiry_date
--order by a.symbol
order by pcr
