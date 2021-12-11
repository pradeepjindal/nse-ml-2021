select tdy.symbol || ' | ' || to_char(yes.trade_Date,'MON-dd') || ' | ' || to_char(tdy.trade_Date,'MON-dd') || ' | ' || to_char(tdy.expiry_Date,'MON-dd') || ' | ' || cm.close AS sym_yd_td_ed_close
, tdy.c_hlm, yes.c_oic yes_C_oic, tdy.c_oic tdy_C_oic, tdy.c_oi tdy_c_oi, tdy.strike, tdy.p_oi tdy_p_oi, tdy.p_oic tdy_p_oic, yes.p_oic yes_p_oic, tdy.p_hlm
from
(
	select
	ce.symbol, ce.trade_date, ce.expiry_date, '--||--' wall, ce.option_type c_ot, ce.HLM c_HLM, ce.oic c_oic, ce.oi c_oi, ce.strike, pe.oi p_oi, pe.oic p_oic, pe.HLM p_hlm, pe.option_type p_ot
	from
	(
		select symbol, trade_date, expiry_date, option_type, strike_price strike, high-low as HLM, open_int/2900 oi, change_in_oi/2900 oic
		from nse_option_market_tab where symbol = 'INDIACEM' and option_type = 'CE' and trade_Date = to_Date('2021-11-02','yyyy-MM-dd') and expiry_Date = to_Date('2021-11-25','yyyy-MM-dd')
		order by strike_price
	) ce,
	(
		select symbol, trade_date, expiry_date, option_type, strike_price strike, high-low as HLM, open_int/2900 oi, change_in_oi/2900 oic
		from nse_option_market_tab where symbol = 'INDIACEM' and option_type = 'PE' and trade_Date = to_Date('2021-11-02','yyyy-MM-dd') and expiry_Date = to_Date('2021-11-25','yyyy-MM-dd')
		order by strike_price
	) pe
	where ce.strike = pe.strike
	and (ce.hlm <> 0 or pe.hlm <> 0)
	--and ce.strike > 290 and ce.strike < 430
) tdy,
(
	select
	ce.symbol, ce.trade_date, ce.expiry_date, '--||--' wall, ce.option_type c_ot, ce.HLM c_HLM, ce.oic c_oic, ce.oi c_oi, ce.strike, pe.oi p_oi, pe.oic p_oic, pe.HLM p_hlm, pe.option_type p_ot
	from
	(
		select symbol, trade_date, expiry_date, option_type, strike_price strike, high-low as HLM, open_int/2900 oi, change_in_oi/2900 oic
		from nse_option_market_tab where symbol = 'INDIACEM' and option_type = 'CE' and trade_Date = to_Date('2021-11-01','yyyy-MM-dd') and expiry_Date = to_Date('2021-11-25','yyyy-MM-dd')
		order by strike_price
	) ce,
	(
		select symbol, trade_date, expiry_date, option_type, strike_price strike, high-low as HLM, open_int/2900 oi, change_in_oi/2900 oic
		from nse_option_market_tab where symbol = 'INDIACEM' and option_type = 'PE' and trade_Date = to_Date('2021-11-01','yyyy-MM-dd') and expiry_Date = to_Date('2021-11-25','yyyy-MM-dd')
		order by strike_price
	) pe
	where ce.strike = pe.strike
	and (ce.hlm <> 0 or pe.hlm <> 0)
	--and ce.strike > 290 and ce.strike < 430
	order by ce.strike desc
) yes,
(
	select symbol, trade_date, close from nse_cash_market_tab where trade_Date = to_Date('2021-11-02','yyyy-MM-dd')
) cm
where tdy.strike = yes.strike and tdy.symbol = cm.symbol
order by tdy.strike desc
