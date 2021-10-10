--select *
select
ce.symbol, ce.trade_date, ce.expiry_date, '--||--' wall, ce.option_type ot, ce.HLM, ce.oic, ce.oi, ce.strike, pe.oi, pe.oic, pe.HLM, pe.option_type ot

from
(
	select symbol, trade_date, expiry_date, option_type, strike_price strike, high-low as HLM, open_int/2800 oi, change_in_oi/2800 oic
	from nse_option_market_tab where symbol = 'TATAMOTORS' and option_type = 'CE' and trade_Date = to_Date('2021-10-08','yyyy-MM-dd')  and expiry_Date = to_Date('2021-10-28','yyyy-MM-dd')
	order by strike_price
) ce,
(
	select symbol, trade_date, expiry_date, option_type, strike_price strike, high-low as HLM, open_int/2800 oi, change_in_oi/2800 oic
	from nse_option_market_tab where symbol = 'TATAMOTORS' and option_type = 'PE' and trade_Date = to_Date('2021-10-08','yyyy-MM-dd')  and expiry_Date = to_Date('2021-10-28','yyyy-MM-dd')
	order by strike_price
) pe
where ce.strike = pe.strike and ce.strike > 290 and ce.strike < 430
order by ce.strike
